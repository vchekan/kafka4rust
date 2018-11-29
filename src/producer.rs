use futures::future::Future;
use cluster::Cluster;
use std::sync::{Arc, Mutex};
use futures::prelude::*;
use futures::sync::BiLock;
use std::collections::{VecDeque, HashMap};
use futures::future::Either;

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
/// brokers with smller load) have to wait for slower ones. Thus, alternative design 2
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
/// Metadata update
///
/// Design consideration:
/// Pass topic(s) to Producer initialization and refuse to send message
/// to uninitialized topics or dynamically discover topic metadata when seen topic for the first time?
/// When broker failover happen, we need to re-discover topic metadata anyway, so we are not simplifying anything
/// by binding topic to producer at init time.
///
/// Q: Should `Producer` connect to seed brokers upon initialization or upon 1st message arrival? A:
/// upon message arrival, because we decided that we do not associate topic(s) with `Producer` and
/// topic needs discovery and maybe connection anyway.
///
/// Q: what is the type parameter of internal buffers? A: Vec<u8>. We do serialization first and
/// store only serialized representation of message. This removes need to parameterise buffers.
///
/// Topic metadata discovery design: when new topic is required or partition has failed, a message is sent to
/// topic metadata discovery process. When Metadata discovery process receive topic metadata, it
/// publish it to Producer, which in turn, updates Buffer.
///
/// What Produce is listening to:
///     Topic resolution
///     buffer overflow event
///     low watermark?
///     Message ack?
struct Producer {
    cluster: Cluster,
    buffer: BiLock<Buffer>,
    topic_meta: HashMap<String, Vec<PartitionMeta>>,
    unrouted_messages: VecDeque<QueuedMessage>,
}

const UNROUTED_BUFFER_MAX_MESSAGES: usize = 100;

/// Serialized message with topic and partition preserved because we need them in case topic
/// resolved or topology change.
struct QueuedMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    topic: String,
    partition: u32,
}

trait ToMessage {
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Vec<u8>;
}

trait Partitioner<M> where M: ToMessage {
    fn partition(message: &M) -> u32;
}

impl Producer {
    fn new(seed: &str) -> Self {
        let (producerLock, timerLock) = BiLock::new(Buffer::new());
        Self::start_timer(timerLock);
        let cluster = Cluster::new(vec![seed.to_string()]);
        let topic_meta = HashMap::new();
        let unrouted_messages = VecDeque::new();
        Producer {cluster, buffer: producerLock, topic_meta, unrouted_messages}
    }

    /// Returns internal buffer status. If `false`, then internal buffer is overflown and `on_low_watermark` should be called
    /// to await for. If `true`, then buffer has space and more messages can be sent.
    /// Produce call will not send message but only buffer it. Sending message will happen periodically by internal timer.
    /// You can listen to sent message acknowledgement by listening to `on_message_ack`
    fn produce<T,P>(&mut self, msg: &T, topic: String) //-> impl Future<Item=(),Error=()>
        where T: ToMessage,
              P: Partitioner<T>
    {
        let partition = P::partition(msg);
        let msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            topic,
            partition
        };

        /*
        match self.topic_meta.get(&topic) {
            Some(meta) => {
                // TODO: Maybe spinning lock would be Ok if contention is low?
                let x = self.buffer.lock().map(|mut buffer|{
                    //buffer.add(&msg, &topic)
                    ()
                });
                Either::A(x)
            },
            None => {
                self.unrouted_messages.push_back(msg);
                if self.unrouted_messages.len() > UNROUTED_BUFFER_MAX_MESSAGES {
                    Either::B(futures::future::ok(()))
                } else {
                    Either::B(futures::future::ok(()))
                }
            }
        }
        */
    }

    // TODO: do it as OneTimeShot
    //fn on_low_watermark() -> impl Future<Item=(), Error=()> {}

    // TODO: bounded stream?
    //fn on_message_ack() -> {}

    //fn close(&mut self) -> impl Future<Item=(), Error=String> {}

    fn start_timer(timer_lock: BiLock<Buffer>) {

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

impl Buffer
{
    fn new() -> Self {Buffer {topics: HashMap::new()}}

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    fn add<M>(&mut self, msg: &M, topic: &String) where M: ToMessage {

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
struct MetadataDiscovery {

}

impl MetadataDiscovery {
    // TODO: how to stop when Producer is closed?
    // If recovery is in progress, it will prevent application from exiting.
    fn start(topic: &String) {

    }


}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let seed = "127.0.0.1:9092";
        let topic = "test1";
        let producer = Producer::new(&seed);
        /*Stream(1..100).map(|i| {
            let msg = sprintf("i:{}", i);
            producer.produce(msg)
        });*/

        //producer.close().wait();
    }
}