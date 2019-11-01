use crate::cluster::Cluster;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use async_std::sync::{Arc, Mutex};
use crate::error::Result;
use crate::protocol;
use async_std::task;
use async_std::task::JoinHandle;
use failure::_core::time::Duration;
use std::time::UNIX_EPOCH;

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
///    client
///      ^
///      |
///      v
///   producer/buffer <-----o
///      ^                  |
///      |                  |
///      v                  |
///   topic resolver <--o   |
///                     |   |
///                     v   v
///                broker connection
///
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
/// Q: Who is doing partitioning, buffer or Producer?
/// A: unresolved data can be stored in unresolved buffer. Then it makes more sense to do
/// partitioning in buffer. But on the other side buffering of unresolved, it might be considered
/// a "buffer bloat" antipattern, robbing application from useful information and possibility to act.
///
/// Topic metadata discovery design: when new topic is required or partition has failed, a message is sent to
/// topic metadata discovery process. When Metadata discovery process receive topic metadata, it
/// publish it to Producer, which in turn, updates Buffer.
///

pub trait ToMessage: Send {
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Vec<u8>;
}

pub trait Partitioner<M>
where
    M: ToMessage,
{
    fn partition(message: &M) -> u32;
}

/// Public Producer structure
#[derive(Debug)]
pub struct Producer
{
    // Buffer is protected because it is being mutated by appending new message called from
    // client to append new messages and from send process to delete sent messages.
    // It is behind `Arc` because it is used also by sending spawn.
    // TODO: maybe RWLock is better, because while sending, no change occur and truncate and append
    // are short.
    buffer : Arc<Mutex<Buffer>>,
    // TODO: is kafka topic case-sensitive?
    topics_meta : TopicMetaCache,
    //topics_meta : HashMap<String,protocol::MetadataResponse0>,
    cluster : Cluster,
    buffer_handler: JoinHandle<()>,
}

type TopicMetaCache = HashMap<String,protocol::MetadataResponse0>;

//
// Implementations
//
impl Producer {
    pub async fn new(seed: &str) -> Result<Self> {
        let buffer = Arc::new(Mutex::new(Buffer::new()));
        let producer = Producer {
            //cmd_channel,
            buffer : buffer.clone(),
            topics_meta : HashMap::new(),
            cluster : Cluster::connect(vec![seed.to_string()]).await?,
            buffer_handler: task::spawn(Buffer::run_loop(buffer) ),
        };

        Ok(producer)
    }

    pub async fn send<P, M>(&mut self, msg: M, topic: String) -> Result<()>
        where P: Partitioner<M>,
        M: ToMessage
    {

        let meta = Self::get_or_request_meta(&self.cluster, &mut self.topics_meta, &topic).await?;
        assert_eq!(1, meta.topics.len());
        assert_eq!(topic, meta.topics.get(0).unwrap().topic);
        let meta = meta.topics.get(0).unwrap();

        let partitions_count = meta.partition_metadata.len() as u32;
        let partition = P::partition(&msg) % partitions_count;
        // TODO: make buffer locking more granular. While topic is being resolved,
        // no new messages can be appended and no datarecords can be sent to broker.
        let mut buffer = self.buffer.lock().await;
        buffer.ensure_topic(&meta);
        match buffer.add(&msg, topic, partition, partitions_count) {
            BufferingResult::Ok => {},
            BufferingResult::Overflow => {
                // TODO:
                unimplemented!()
            }
        }
        Ok(())
    }

    // TODO: `close()` can take self thus avoid possibility to call `send` after `close`
    async fn close(mut self) -> Result<()> {
        // TODO: await for buffer flush
        self.buffer_handler.await;
        Ok(())
    }

    /// Get topic's meta from cache or request from broker.
    /// Result will be cached.
    /// `self` is not passed as a prarameter because it causes lifetime conflict in `send`.
    /// TODO: what is retry policy?
    /// TODO: what to do if failed for extended period of time?
    async fn get_or_request_meta<'a>(cluster: &'_ Cluster, topics_meta: &'a mut TopicMetaCache, topic: &'_ String) -> Result<&'a protocol::MetadataResponse0> {

        // `if let Some(meta) = self.topics_meta.get(topic)` does not work because of lifetimes,
        // so have to do 2 lookups :(
        if topics_meta.contains_key(topic) {
            let mut topic_meta = topics_meta;
            let meta = topic_meta.get(topic).unwrap();
            return Ok(meta);
        }

        let meta = cluster.resolve_topic(topic).await?;
        topics_meta.insert(topic.clone(), meta);
        let meta = topics_meta.get(topic).unwrap();
        return Ok(meta);

    }
}

/// Q: should buffer data be shared or copied when sending to broker?
/// A:
///
///
///                 | partition1 queue<messages>
///        | topic1-| partition2 queue<messages>
///        |        | partition3 queue<messages>
/// Buffer-|
///        |        | partition1 queue<messages>
///        | topic2-| partition2 queue<messages>
///
///
///
///                                | topic 1 --|partition 0; recordset
/// ProduceMessage --| broker_id 1 |           |partition 1; recordset
///                  |
///                  | broker_id 2 | topic 2 --| partition 0; recordset
///                                |
///                  |             | topic 3 --| partition 0; recordset
/// 
#[derive(Debug)]
struct Buffer {
    topics: HashMap<String, Vec<PartitionQueue>>,
    bytes : u32,
    size_limit : u32,
    /// Vector of partitions
    meta_cache : HashMap<String, Vec<BrokerId>>
}

type PartitionQueue = VecDeque<QueuedMessage>;
type BrokerId = i32;

/// Serialized message with topic and partition preserved because we need them in case topic
/// resolved or topology change.
#[derive(Debug)]
pub(crate) struct QueuedMessage {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: u64,
}

#[derive(PartialEq, Debug)]
enum BufferingResult {
    Ok,
    Overflow
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            topics: HashMap::new(),
            bytes : 0,
            // TODO: make configurable
            size_limit : 100 * 1024 * 1024,
            meta_cache: HashMap::new(),
        }
    }

    fn ensure_topic(&mut self, meta: &protocol::TopicMetadata) {
        if self.meta_cache.contains_key(&meta.topic) {
            return;
        }

        self.meta_cache.insert(meta.topic.clone(), meta.partition_metadata.iter().map(|p| p.leader).collect());
    }

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    fn add<M>(&mut self, msg: &M, topic: String, partition: u32, partitions_count: u32) -> BufferingResult
        where M: ToMessage,
    {
        if self.bytes + msg.value().len() as u32 > self.size_limit {
            debug!("Overflow");
            return BufferingResult::Overflow;
        }

        let partitions = self.topics.entry(topic).or_insert_with(|| {
            (0..partitions_count).map(|_| PartitionQueue::new()).collect()
        });

        self.bytes += (msg.value().len() + msg.key().map(|k| k.len()).unwrap_or(0)) as u32;

        // TODO: would it be possible to keep reference to the message data instead of cloning? Would it be possible to do both?
        let msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            timestamp: std::time::SystemTime::now().
                duration_since(UNIX_EPOCH).expect("Failed to get timestamp").
                as_millis() as u64
        };
        partitions[partition as usize].push_back(msg);
        debug!("Added message");
        BufferingResult::Ok
    }

    async fn run_loop(buffer: Arc<Mutex<Self>>) {
        loop {
            // TODO: make delay configurable
            // TODO: init send upon treshold or timeout
            task::sleep(Duration::from_secs(10)).await;
            debug!("Buffer timer woke up");
        }
    }

    fn mk_recordset(&self) {
        // TODO: implement FIFO and request size bound algorithm

        /*
        let mut topic_data = vec![protocol::TopicProduceData {
            topic: topic.clone(),
            data: protocol::ProduceData {
                partition: 0,
                record_set: vec![],
            },
        }];
        */

        debug!("self.topics: {:?}", self.topics);
        // broker_id->topic->partition->recordset[]
        let mut broker_partitioned = HashMap::<i32, HashMap<&String, HashMap<u32, (&[QueuedMessage], &[QueuedMessage])>>>::new();
        self.topics.iter().for_each(|(topic, partitions)| {
            // N-th partition in queue match to partition number itself, so use `enumerate()`
            // instead of storing partition number in the queue
            let partition_message = partitions.iter().enumerate().
                // Only non-empty queues
                filter(|(_,q)| q.len() > 0).
                for_each(|(partition, queue)| {
                    let partition = partition as u32;
                    let broker_id = self.meta_cache.get(topic).expect("Topic metadata is expected").
                        get(partition as usize).expect("Corrupt topic metadata partition info");

                    // TODO use raw entries
                    let topics = broker_partitioned.entry(*broker_id).or_insert_with(|| HashMap::new());
                    let partitions = topics.entry(topic).or_insert_with(|| HashMap::new());
                    assert!(partitions.insert(partition, queue.as_slices()).is_none());
                    //let recordsets = partitions.entry(partition).or_insert_with(||vec![]);
                    //let x = queue.as_slices();
                    //recordsets.push(x);
                });
        });

        /*
        let request = protocol::ProducerRequest0 {
            acks: 0,        // TODO: config
            timeout: 3000,  // TODO: config
            topic_data: &broker_partitioned
        };
        */

        debug!("Recordset: {:?}", broker_partitioned);

            /*protocol::TopicProduceData {
                topic: topic.clone(),
                data: protocol::ProduceData {
                    partition: 0,
                    record_set: vec![],
                },
            }*/
        //};

        /*
        let set = protocol::ProduceRequest0 {
            acks: 1,
            timeout: 5000,
            topic_data
        };

        set
        */
    }
}



enum TimestampType {
    Create = 0,
    LogAppend = 1,
}



pub struct BinMessage {
    key: Vec<u8>,
    value: Vec<u8>,
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
    value: String,
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
    use async_std::task;
    use std::time::UNIX_EPOCH;

    struct P1 {}
    impl Partitioner<StringMessage> for P1 {
        fn partition(message: &StringMessage) -> u32 {
            5
        }
    }

    #[test]
    fn it_works() -> Result<()> {
        task::block_on(async {
            simple_logger::init_with_level(log::Level::Debug)?;
            let seed = "127.0.0.1:9092";
            let _topic = "test1";

            let producer = Producer::new(&seed).await?;
            let mut producer = producer;
            for i in 1..100 {
                let msg = format!("i:{}", i);
                let msg = StringMessage {
                    key: i.to_string(),
                    value: msg,
                };
                producer.send::<P1,_>(msg, "topic1".to_string()).await;
            }
            debug!("{:?}", producer);
            producer.close().await.expect("Failure when closing producer");

            Ok(())
        })
    }

    #[test]
    fn mk_batch() {

        simple_logger::init_with_level(log::Level::Debug).unwrap();
        let mut buffer = Buffer::new();
        buffer.ensure_topic(&protocol::TopicMetadata {
            topic: "topic 1".to_owned(),
            error_code: 0,
            partition_metadata: vec![
                protocol::PartitionMetadata{
                    error_code: 0,
                    partition: 0,
                    leader: 0,
                    replicas: 1,
                    isr: 1,
                },
             protocol::PartitionMetadata{
                error_code: 0,
                partition: 1,
                leader: 0,
                replicas: 1,
                isr: 1,
            }],
        });
        buffer.ensure_topic(&protocol::TopicMetadata {
            topic: "topic 2".to_owned(),
            error_code: 0,
            partition_metadata: vec![
                protocol::PartitionMetadata {
                error_code: 0,
                partition: 0,
                leader: 0,
                replicas: 1,
                isr: 1,
            }]
        });

        assert_eq!(buffer.add(&mk_msg(&"msg 1"), "topic 1".to_owned(), 0, 3), BufferingResult::Ok);
        assert_eq!(buffer.add(&mk_msg(&"msg 2"), "topic 1".to_owned(), 0, 3), BufferingResult::Ok);
        assert_eq!(buffer.add(&mk_msg(&"msg 3"), "topic 1".to_owned(), 1, 3), BufferingResult::Ok);
        assert_eq!(buffer.add(&mk_msg(&"msg 4"), "topic 2".to_owned(), 0, 3), BufferingResult::Ok);

        let rs = buffer.mk_recordset();
        //debug!("Recordset: {:?}", rs);
    }

    fn mk_msg(msg: &str) -> StringMessage {
        StringMessage {
            key: "1".to_string(),
            value: msg.to_string(),
        }
    }
}
