use std::collections::{HashMap, VecDeque};
use crate::cluster::Cluster;
use std::thread;
use futures::{Future, StreamExt, Sink, SinkExt};
use futures::prelude::*;
use futures::stream::*;
use futures::channel::mpsc;
use futures::executor::LocalPool;
use futures::task::SpawnExt;
use futures::executor::LocalSpawner;
use std::fmt::Debug;
use futures::task::LocalSpawn;

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
/// `Close` design
/// Have `close()` return future?
/// Have data channel to return Closed event?
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

pub trait ToMessage : Send {
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Vec<u8>;
}

trait Partitioner<M>
    where
        M: ToMessage,
{
    fn partition(message: &M) -> u32;
}

/// Public Producer structure
pub struct Producer<M> where M: ToMessage {
    cmd_channel: mpsc::UnboundedSender<Event<M>>,
    closed: bool,
}

/// Internal Producer structure which is going to be moved to execution loop.
struct ProducerImpl<M> where M: ToMessage {
    cluster: Cluster,
    buffer: Buffer,
    topic_meta: HashMap<String, Vec<PartitionMeta>>,

    events: mpsc::UnboundedReceiver<Event<M>>,
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
impl<M> Producer<M> where M: ToMessage + 'static + Debug {
    pub fn new(seed: &str, pool: &mut LocalSpawner) -> (Self, mpsc::UnboundedReceiver<AppEvent>) {
        // TODO: unbounded is dangerous
        let (app_cmd_channel, events) = mpsc::unbounded();
        let cmd_channel = ProducerImpl::new(seed, app_cmd_channel, pool);
        let producer = Producer { cmd_channel, closed: false };
        (producer, events)
    }

    /// Will panic if called after `close()`
    pub fn send(&mut self, msg: M, topic: String) -> impl Future + '_ {
        if self.closed {
            panic!("Can't call send() after close()");
        }

        self.cmd_channel.send(Event::MessageIn(msg, topic))
    }

    async fn close(&mut self) -> Result<(), mpsc::SendError> {
        self.closed = true;
        await!(self.cmd_channel.send(Event::Close))
    }
}

#[derive(Debug)]
enum Event<M> where M: ToMessage {
    MessageIn(M, String),
    Close,
}

/// Events from driver to app
#[derive(Debug)]
pub enum AppEvent {
    OverflowOn,
    OverflowOff,
    Ack,
    Nack,
    Closed
}

impl<M> ProducerImpl<M> where M: ToMessage + 'static + Debug {
    fn new(seed: &str, app_cmd_channel: mpsc::UnboundedSender<AppEvent>, pool: &mut LocalSpawner) -> mpsc::UnboundedSender<Event<M>> {
        let (data_sender, data_receiver) = mpsc::unbounded();
        let seed = seed.to_string();

        let producer_loop = async {
            debug!("Started producer_loop");
            let cluster = Cluster::new(vec![seed]);
            let topic_meta = HashMap::new();
            let producer = ProducerImpl {
                cluster,
                buffer: Buffer::new(),
                topic_meta,
                events: data_receiver,
            };

            debug!("Listening to events");
            await!(producer.events.
                for_each(move |e| {
                    match e {
                        Event::MessageIn(msg, topic) => debug!("Got message"),
                        Event::Close => {
                            debug!("Got Close");
                            app_cmd_channel.unbounded_send(AppEvent::Closed);
                        },
                    }
                    future::ready(())
                })
            );
        };

        pool.spawn(producer_loop).expect("Spawn failed");
        data_sender
    }

    /*
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

    }*/

    // TODO: do it as OneTimeShot
    //fn on_low_watermark() -> impl Future<Item=(), Error=()> {}

    // TODO: bounded stream?
    //fn on_message_ack() -> {}

    //fn start_timer(_timer_lock: BiLock<Buffer>) {}


    fn handle_message(&mut self, msg: M, topic: String) {
        match self.topic_meta.get(&topic) {
            Some(meta) => {
                //buffer.add(&msg, &topic)
                println!("Got message for topic: {}", topic);
            },
            None => {
                println!("Topic not found: '{}'", topic);
                /*self.unrouted_messages.push_back(msg);
                if self.unrouted_messages.len() > UNROUTED_BUFFER_MAX_MESSAGES {
                    Either::B(futures::future::ok(()))
                } else {
                    Either::B(futures::future::ok(()))
                }
                */
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
/*struct MetadataDiscovery {}

impl MetadataDiscovery {
    // TODO: how to stop when Producer is closed?
    // If recovery is in progress, it will prevent application from exiting.
    fn start(_topic: &String) {}
}
*/

pub struct BinMessage {
    key: Vec<u8>,
    value: Vec<u8>
}

impl ToMessage for BinMessage {
    fn key(&self) -> Option<Vec<u8>> {
        Some(self.key.clone())
    }
    fn value(&self) -> Vec<u8> {
        self.value.clone()
    }
}

#[derive(Debug)]
pub struct StringMessage {
    key: String,
    value: String
}

impl ToMessage for StringMessage {
    fn key(&self) -> Option<Vec<u8>> {
        Some(self.key.as_bytes().to_vec())
    }
    fn value(&self) -> Vec<u8> {
        self.value.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::executor;
    use simplelog::*;

    #[test]
    fn it_works() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
        ]).unwrap();

        let mut localPool: LocalPool = LocalPool::new();
        let mut spawner = localPool.spawner();
         localPool.run_until(async {
            let seed = "127.0.0.1:9092";
            let _topic = "test1";

            let (producer, events) = Producer::<StringMessage>::new(&seed, &mut spawner);
            let mut producer = producer;
            for i in 1..100 {
                let msg = format!("i:{}" , i);
                let msg = StringMessage{key: i.to_string(), value: msg};
                await!(producer.send( msg, "topic1".to_string()));
            };
            await!(producer.close()).expect("Failure when closing producer");

            
            let mut closed = events.
                inspect(|e| println!("producer.events({:?})", e)).
                filter(|e| if let AppEvent::Closed = e {future::ready(true)} else {future::ready(false)});
            let closed = closed.next();
            match await!(closed) {
                Some(AppEvent::Closed) => println!("Got close event"),
                None => println!("Producer events closed without sending Close. Not nice!"),
                x => println!("That's unexpected: {:?}", x)
            } 
        });
    }
}
