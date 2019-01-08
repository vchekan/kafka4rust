use std::sync::mpsc;
use std::collections::{HashMap, VecDeque};
use crate::cluster::Cluster;

/// Producer's design is build around `Buffer`. `Producer::produce()` put message into buffer and
/// internal timer sends messages accumulated in buffer to kafka broker.
/// Backpressure is volunteer operation. If buffer exceeds high watermark, then `produce` will return false
/// and well behaved process should await for `on_low_watermark` event.
///
/// Interaction with Cluster. Producer has lock-able topics metadata information and can find topic's
/// partition count for message partitioning purposes. If buffer limit is reached, `Send` is called
/// and awaited before adding to the buffer. If timer expires, then TODO: how does timer invoke
/// `Send`? Should Producer listen to a channel for incoming messages and timer?
///
/// `Send` design 1
/// `Send` takes up to buffer limit messages (TODO: limit is against compressed or uncompressed?)
/// and groups topic-partition by current leader brokers, so that different partitions which belong
/// to the same broker would be sent in single request. Request size calculation requires a lot of
/// Request awareness, so Buffer constructs `ProduceRequest` with routed broker Id. `Send` takes
/// requests and calls `Cluster::send_parallel` async. But this means that faster brokers (or
/// brokers with smaller load) have to wait for slower ones. Thus, alternative design 2
///
/// `Send` design 2
/// `Send` keeps track of current partition routing and per-broker queue size (in messages or bytes,
/// according to config). Once limit is reached, or timer triggers, ProduceMessage for the broker is
/// formed and `Cluster::send` async is called and awaited. Async result is handled. If success,
/// then messages removed from queue and Ack is sent to application. instrumentation is updated and
/// buffer is made to check for low watermark to resume produce call if awaited. TODO: how to do
/// pub/sub? If Failure, then statistics is updated and retry loop is started according to retry
/// interval.
///
/// Preserving message order
///
/// Metadata update
///
/// Design consideration:
/// Pass topic(s) to Producer initialization and refuse to send message
/// to uninitialized topics or dynamically discover topic metadata when seen topic for the first time?
/// When broker failover happen, we need to re-discover topic metadata anyway, so we are not simplifying anything
/// by binding topic to producer at init time.
///
/// List of all events:
///     * App sent data
///     * App send command (close, pause, get stats, etc)
///     * Batch send complete (success or failure)
///     * Partition recovered
///     * Leaders(metadata) update
///     * Failed topic retry timer
///     * Linger timer
///
/// Q: Should `Producer` connect to seed brokers upon initialization or upon 1st message arrival?
/// A: upon message arrival, because we decided that we do not associate topic(s) with `Producer` and
/// topic needs discovery and maybe connection anyway.
///
/// Q: what is the type parameter of internal buffers? A: Vec<u8>. We do serialization first and
/// store only serialized representation of message. This removes need to parameterise buffers.
///
/// Q: Should Producer be parameterised by message type? If so, it can't be used to send messages of
/// different types.
///
/// Q: should messages be queued to unrouted queue while leader info is not available (first message
/// to topic, leader election)?
/// A: What does it give us? A chance to keep delivering messages for other partitions while putting
/// aside unresolved one? But with high velocity it will exhaust buffer fast, with low velocity it
/// does not matter. So, no, it does not provide substantial benefits.
///
/// Q: How backpressure is implemented sync or async?
/// A: async is preferred, but there is sync bounded and async unbounded channel in rust. Either
/// accept sync backpressure or make your own async bounded one.
/// We can do async overflow notification. Instead of returning status on every `send` call, we can
/// listen to overflow messages from buffer and suspend receiving messages from app when in overflow
/// state.
///
/// Topic metadata discovery design: when new topic is required or partition has failed, a message is sent to
/// topic metadata discovery process. When Metadata discovery process receive topic metadata, it
/// publish it to Producer, which in turn, updates Buffer.
///

//
// Traits
//
trait ToMessage {
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Vec<u8>;
}

trait Partitioner<M>
    where
        M: ToMessage,
{
    fn partition(message: &M) -> u32;
}

//
// Structs
//
struct Producer<M> where M: ToMessage {
    cluster: Cluster,
    buffer: Buffer,
    topic_meta: HashMap<String, Vec<PartitionMeta>>,

    events: mpsc::Receiver<Event<M>>,
    app_cmd_channel: mpsc::Sender<AppEvent>,
}

pub struct ProducerApi<M> {
    cmd_channel: mpsc::SyncSender<Event<M>>,
    events: mpsc::Receiver<AppEvent>
}

/// Serialized message with topic and partition preserved because we need them in case topic
/// resolved or topology change.
struct QueuedMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    topic: String,
    partition: u32,
}

//
// Implementations
//

impl<M> ProducerApi<M> where M: ToMessage {
    pub fn new(seed: &str) -> Self {
        let (app_cmd_channel, events) = mpsc::channel();
        let cmd_channel = Producer::new(seed, app_cmd_channel);
        ProducerApi { cmd_channel, events }
    }

    pub async fn send(&self, msg: M, topic: String) {
        
        await!(self.cmd_in.send(Cmd::MessageIn(msg.ToMessage(), topic)));
    }
}

enum Event<M> where M: ToMessage {
    MessageIn(M, String),
}

/// Events from driver to app
enum AppEvent {
    OverflowOn,
    OverflowOff,
}

impl<M> Producer<M> where M: ToMessage {
    fn new(seed: &str, app_cmd_channel: mpsc::Sender<AppEvent>) -> mpsc::SyncSender<Event<M>> {
        let cluster = Cluster::new(vec![seed.to_string()]);
        let topic_meta = HashMap::new();
        // TODO: measure what is probability of contention, Can buffer size be 0?
        let (data_sender, data_receiver) = mpsc::sync_channel(1);

        Producer {
            cluster,
            buffer: Buffer::new(),
            topic_meta,
            events: data_receiver,
            app_cmd_channel,
        }.start_execution_loop();

        data_sender
    }

    /// Produce call will not send message but only buffer it. Sending message will happen
    /// periodically by internal timer or when buffer overflow. You can listen to sent message
    /// acknowledgement by listening to `on_message_ack`.
    pub async fn enqueue_message<T, P>(&mut self, msg: &T, topic: String)
    where
        T: ToMessage,
        P: Partitioner<T>,
    {
        let partition = P::partition(msg);
        let _msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            topic,
            partition,
        };

    }

    // TODO: do it as OneTimeShot
    //fn on_low_watermark() -> impl Future<Item=(), Error=()> {}

    // TODO: bounded stream?
    //fn on_message_ack() -> {}

    //fn close(&mut self) -> impl Future<Item=(), Error=String> {}

    //fn start_timer(_timer_lock: BiLock<Buffer>) {}

    fn start_execution_loop(mut self) {
        loop {
            match await!(self.cmd_out) {
                Event::MessageIn(msg, topic) => handle_message(msg, topic),
            }
        }
    }

    fn handle_message(&mut self, M: msg, topic: String) {
        match self.topic_meta.get(&topic) {
            Some(meta) => buffer.add(&msg, &topic),
            None => {
                self.unrouted_messages.push_back(msg);
                if self.unrouted_messages.len() > UNROUTED_BUFFER_MAX_MESSAGES {
                    Either::B(futures::future::ok(()))
                } else {
                    Either::B(futures::future::ok(()))
                }
            }
        }
    }
}

///                 | partition1 queue<messages>
///        | topic1-| partition2 queue<messages>
///        |        | partition3 queue<messages>
/// Buffer-|
///        |        | partition1 queue<messages>
///        | topic2-| partition2 queue<messages>
///
struct Buffer {
    topics: HashMap<String, Vec<PartitionQueue>>,
}

struct PartitionQueue {
    messages: VecDeque<Vec<u8>>,
}

struct PartitionMeta {
    leader: i32,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            topics: HashMap::new(),
        }
    }

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    fn add<M>(&mut self, _msg: &M, _topic: &String)
    where
        M: ToMessage,
    {
        unimplemented!();
        /*let partition = P::parttion(msg) % partition_count;
        let partitions = match self.topics.get(topic) {
            Some(partitions) => partitions,
            None => {
                let partitions = vec![];
                self.topics.insert(partitions);
            }
        };*/
    }
}

//
// Metadata discovery process
//
struct MetadataDiscovery {}

impl MetadataDiscovery {
    // TODO: how to stop when Producer is closed?
    // If recovery is in progress, it will prevent application from exiting.
    fn start(_topic: &String) {}
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let seed = "127.0.0.1:9092";
        let _topic = "test1";
        let _producer = Producer::new(&seed);
        /*Stream(1..100).map(|i| {
            let msg = sprintf("i:{}", i);
            producer.produce(msg)
        });*/

        //producer.close().wait();
    }
}
