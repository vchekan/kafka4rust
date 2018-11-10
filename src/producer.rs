use futures::future::Future;
use cluster::Cluster;
use std::sync::{Arc, Mutex};
use futures::sync::BiLock;
use std::collections::{VecDeque, HashMap};

/// Producer's design is build around `Buffer`. `Producer::produce()` put message into buffer and
/// internal timer sends messages accumulated in buffer to kafka broker.
/// Backpressure is volunteer operation. If buffer exceeds high watermark, then `produce` will return false
/// and well behaved process should await for `on_low_watermark` event.
///
/// Design consideration: Pass topic(s) to Producer initialization and refuse to send message
/// to uninitialized topics or dynamically discover topic metadata when seen topic for the first time?
/// When broker failover happen, we need to re-discover topic metadata anyway, so we are not simplifying anything
/// by binding topic to producer at init time.
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
}

trait ToMessage {
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> &[u8];
}

trait Partitioner<M> where M: ToMessage {
    fn partition(message: &M) -> u32;
}

impl Producer {
    fn connect(seed: &str, topic: &str) -> impl Future<Item=Self, Error=String> {
        let (producerLock, timerLock) = BiLock::new(Buffer::new());
        Self::start_timer(timerLock);
        Cluster::connect(&vec![seed], &vec![topic]).
            map(|cluster| Producer {cluster, buffer: producerLock})
    }

    /// Returns internal buffer status. If `false`, then internal buffer is overflown and `on_low_watermark` should be called
    /// to await for. If `true`, then buffer has space and more messages can be sent.
    /// Produce call will not send message but only buffer it. Sending message will happen periodically by internal timer.
    /// You can listen to sent message acknowledgement by listening to `on_message_ack`
    fn produce<T>(&mut self, msg: T) -> bool
        where T: ToMessage
    {
        // TODO: is it possible to implement non-blocking DeQueue? Then `produce` can be implemented without Future
        let lock = self.buffer.lock();
        
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
    topic_meta: HashMap<String, Vec<PartitionMeta>>
}

struct PartitionQueue {
    messages: VecDeque<Vec<u8>>,
}

struct PartitionMeta {
    leader: i32,
}

impl Buffer
{
    fn new() -> Self {Buffer {topics: vec![], topic_meta: HashMap::new()}}

    /*fn add<M>(&mut self, msg: &M, topic: &String) where M: ToMessage {
        // Need partition count for given topic.
        let partition_count = 1;

        let partition = P::parttion(msg) % partition_count;
        let partitions = match self.topics.get(topic) {
            Some(partitions) => partitions,
            None => {
                let partitions = vec![];
                self.topics.insert(partitions)
            }
        }
    }*/
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
        let producer = Producer::connect(&seed, &topic);
        /*Stream(1..100).map(|i| {
            let msg = sprintf("i:{}", i);
            producer.produce(msg)
        });*/

        producer.close().wait();
    }
}