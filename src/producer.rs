use crate::types::*;
use crate::cluster::Cluster;
use crate::error::{Result};
use crate::protocol;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::time::{UNIX_EPOCH, Duration};
use async_std::sync::{Sender};
use async_std::prelude::*;
use futures::stream::FuturesUnordered;
use std::iter::FromIterator;

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

// TODO: is `Send` needed? Can we conver to QueuedMessage before crossing thread boundaries?
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

pub struct Producer<M: ToMessage> {
    msg_sender: Sender<Cmd<M>>,
}

impl<M: ToMessage + 'static> Producer<M> {
    pub async fn new<P: Partitioner<M>>(seed: &str) -> Result<Self> {
        let msg_sender = ProducerImpl::new::<M,P>(seed).await?;
        let producer = Producer {
            msg_sender
        };
        Ok(producer)
    }

    pub async fn send(&mut self, msg: M, topic: String) {
        self.msg_sender.send(Cmd::Send(msg, topic)).await
    }
}

/// Public Producer structure
#[derive(Debug)]
struct ProducerImpl {
    buffer: Buffer,
    cluster: Cluster,
    // TODO: is kafka topic case-sensitive?
    topics_meta: TopicMetaCache,
    //topics_meta : HashMap<String,protocol::MetadataResponse0>,
    //buffer_handler: RemoteHandle<()>,
}

enum Cmd<M> {
    Send(M, String),
    BufferTimer,
}

type TopicMetaCache = HashMap<String, protocol::MetadataResponse0>;

//
// Implementations
//
impl ProducerImpl {
    pub async fn new<M: ToMessage + 'static, P: Partitioner<M>>(seed: &str) -> Result<Sender<Cmd<M>>> {
        let cluster = Cluster::connect(vec![seed.to_string()]).await?;
        let buffer = Buffer::new();

        let (tx, rx) = async_std::sync::channel(1);
        let tx2 = tx.clone();

        let mut producer = ProducerImpl {
            buffer,
            cluster,
            topics_meta: HashMap::new(),
            // TODO: implement spawn_local_with_handler and
            //buffer_handler: spawn_local(Self::run_loop(buffer))?,
        };

        tokio_current_thread::spawn(async move {
                producer.worker_loop::<M,P>(rx).await;
            }
        );
        tokio_current_thread::spawn(async move {
            loop {
                async_std::task::sleep(Duration::from_secs(5)).await;
                tx2.send(Cmd::BufferTimer).await;
            }
        });

        Ok(tx)
    }

    async fn worker_loop<M: ToMessage, P: Partitioner<M>>(&mut self, messages: async_std::sync::Receiver<Cmd<M>>) {
        debug!("Producer worker loop started");
        loop {
            match messages.recv().await {
                None => {
                    info!("Exiting worker_loop");
                    return;
                },
                Some(Cmd::Send(msg, topic)) => {
                    debug!("worker_loop: got message");
                    match self.send::<P,M>(msg, topic).await {
                        Ok(()) => {},
                        Err(e) => {
                            debug!("Send failed: {:?}", e);
                            // TODO return error on response channel to the caller
                            unimplemented!()
                        }
                    }


                    /*
                    match self.buffer.add(msg, topic, partition, partitions_count) {
                        BufferingResult::Ok => {}
                        BufferingResult::Overflow => {
                            // TODO:
                            unimplemented!()
                        }
                    }
                    */
                }
                Some(Cmd::BufferTimer) => {
                    debug!("Buffer timer");
                    self.buffer.flush(&mut self.cluster).await;
                }
            }
        }
    }

    pub async fn send<P,M>(&mut self, msg: M, topic: String) -> Result<()>
    where
        P: Partitioner<M>,
        M: ToMessage,
    {
        // TODO: share metadata cache between self and Buffer
        debug!(">1");
        let meta = Self::get_or_request_meta(&mut self.cluster, &mut self.topics_meta, &mut self.buffer.meta_cache, &topic).await?;
        debug!(">2");
        assert_eq!(1, meta.topics.len());
        assert_eq!(topic, meta.topics.get(0).unwrap().topic);
        let meta = meta.topics.get(0).unwrap();

        let partitions_count = meta.partition_metadata.len() as u32;
        let partition = P::partition(&msg) % partitions_count;

        // TODO: would it be possible to keep reference to the message data instead of cloning? Would it be possible to do both?
        let msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            timestamp: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get timestamp")
                .as_millis() as u64,
        };
        debug!(">3");
        //self.msg_sender.send(Cmd::Send(msg)).await;
        debug!(">4");
        //self.buffer.ensure_topic(&meta);
        match self.buffer.add(msg, topic, partition, meta) {
            BufferingResult::Ok => {}
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
        //self.buffer_handler.await;
        Ok(())
    }

    /// Get topic's meta from cache or request from broker.
    /// Result will be cached.
    /// `self` is not passed as a prarameter because it causes lifetime conflict in `send`.
    /// TODO: what is retry policy?
    /// TODO: what to do if failed for extended period of time?
    async fn get_or_request_meta<'a>(
        cluster: &mut Cluster,
        topics_meta: &'a mut TopicMetaCache,
        buffer_meta: &mut HashMap<String, Vec<BrokerId>>,
        topic: & String,
    ) -> Result<&'a protocol::MetadataResponse0> {
        // `if let Some(meta) = self.topics_meta.get(topic)` does not work because of lifetimes,
        // so have to do 2 lookups :(
        if topics_meta.contains_key(topic) {
            let mut topic_meta = topics_meta;
            let meta = topic_meta.get(topic).unwrap();
            return Ok(meta);
        }

        // Did not have meta. Fetch and cache.
        let meta = cluster.resolve_topic(topic).await?;
        buffer_meta.insert(topic.clone(), meta.topics[0].partition_metadata.iter().map(|p| p.leader).collect());
        topics_meta.insert(topic.clone(), meta);

        let meta = topics_meta.get(topic).unwrap();
        return Ok(meta);
    }

    /*
    async fn run_loop(buffer: Rc<RefCell<Buffer>>) {
        debug!("Run loop timer started");
        loop {
            // TODO: make delay configurable
            // TODO: init send upon treshold or timeout
            async_std::task::sleep(Duration::from_secs(5)).await;
            debug!("Run loop timer woke up");
            {
                /*let request_starter = | broker_id: BrokerId | {
                    write_request()
                };*/
                let requests = buffer.borrow_mut().mk_requests().await;
                
                /*
                for (broker_id, request) in &requests {

                }
                */
            }
        }
    }
    */

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
    bytes: u32,
    size_limit: u32,
    /// Vector of partitions
    meta_cache: HashMap<String, Vec<BrokerId>>,
    //cluster: &'a Cluster,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            topics: HashMap::new(),
            bytes: 0,
            // TODO: make configurable
            size_limit: 100 * 1024 * 1024,
            meta_cache: HashMap::new(),
        }
    }

    async fn get_or_fetch_topic_meta(&self, topic: &String, cluster: &Cluster) -> Result<&Vec<BrokerId>> {
        unimplemented!()
        /*
            let res = self.meta_cache.get(topic);
            if let Some(res) = res {
                return Ok(res);
            }

            let meta = cluster.resolve_topic(topic).await?;
            // TODO: make sure response has no errors
            assert_eq!(1, meta.topics.len());
            assert_eq!(topic.as_str(), meta.topics[0].topic);

            self.meta_cache.insert(
                topic.clone(),
                meta.topics[0].partition_metadata.iter().map(|p| p.leader).collect(),
            );

            Ok(self.meta_cache.get(topic).unwrap())
        */
    }

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    fn add(
        &mut self,
        msg: QueuedMessage,
        topic: String,
        partition: u32,
        //partitions_count: u32,
        meta: &protocol::TopicMetadata,
    ) -> BufferingResult
    {
        if self.bytes + msg.value.len() as u32 > self.size_limit {
            debug!("Overflow");
            return BufferingResult::Overflow;
        }

        let partitions_count = meta.partition_metadata.len();
        let partitions = self.topics.entry(topic).or_insert_with(|| {
            (0..partitions_count)
                .map(|_| PartitionQueue::new())
                .collect()
        });

        self.bytes += (msg.value.len() + msg.key.as_ref().map(|k| k.len()).unwrap_or(0)) as u32;

        partitions[partition as usize].push_back(msg);
        debug!("Added message");
        BufferingResult::Ok
    }

    async fn flush(&self, cluster: &mut Cluster) -> Result<()> {
        let broker_partitioned = self.mk_requests(cluster);

        for (broker_id, data) in broker_partitioned {
            let request = protocol::ProduceRequest0 {
                acks: 1,
                timeout: 1500,
                topic_data: &data
            };
            let broker = cluster.broker_by_id(broker_id).
                expect(format!("Can not find broker_id {}", broker_id).as_str());
            let cmd_buf = broker.mk_request(request);
            debug!("Sending Produce");
            let res = broker.send_request2::<protocol::ProduceResponse0>(cmd_buf).await?;
            debug!("Got Produce response: {:?}", res);
        }

        /*
        let requests = broker_partitioned.into_iter().map(|(broker_id, data)| {
            let request = protocol::ProduceRequest0 {
                acks: 1,
                timeout: 10_000,
                topic_data: &data
            };
            let r = cluster.broker_by_id(broker_id).
                expect(format!("Can not find broker_id {}", broker_id).as_str());
            let rr = r.mk_request(request);
            (rr, r)
            //()

            /*
            match cluster.broker_by_id(*broker_id) {
                Ok(broker) => {
                    broker.request(&request)
                },
                Err(e) => {
                    // Should not happen
                    panic!("Can not find broker_id {}", broker_id);
                },
            }
            */
        }).collect::<Vec<_>>();

        let x = FuturesUnordered::from_iter(requests.into_iter().map(|(buf, broker)| {
            broker.send_request2::<protocol::ProduceResponse0>(buf)
        }));
        */

        Ok(())
    }

    /// Scan buffer and make a request for each broker_id which has messages in queue
    fn mk_requests(&self, cluster: &Cluster) -> HashMap<BrokerId,HashMap<&String, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>>> {
        // TODO: implement FIFO and request size bound algorithm

        // broker_id->topic->partition->recordset[]
        let mut broker_partitioned = HashMap::<
            BrokerId,
            HashMap<&String, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>>,
        >::new();

        let slices_per_broker: Vec<_> = self.topics.iter().
            flat_map(|(topic, partitions)| {
                // N-th partition in queue match to partition number itself, so use `enumerate()`
                // instead of storing partition number in the queue
                partitions.iter().enumerate().
                    // Only non-empty queues
                    filter(|(_, q)| q.len() > 0).
                    map( move |(partition, queue)| {
                        let partition = partition as u32;
                        //let topic_meta = self.get_or_fetch_topic_meta(topic, cluster).await?;
                        let broker_id = *self.meta_cache.
                            get(topic).expect("Topic metadata is expected").
                            get(partition as usize).expect("Corrupt topic metadata partition info");

                        // TODO use raw entries
                        //let topics = broker_partitioned.entry(broker_id).or_insert_with(|| HashMap::new());
                        //let partitions = topics.entry(topic).or_insert_with(|| HashMap::new());
                        //partitions.insert(partition, queue.as_slices());
                        (broker_id, topic, partition, queue.as_slices())
                    })
            }).collect();

        for (broker_id, topic, partition, slices) in slices_per_broker {
            let topics = broker_partitioned.entry(broker_id).or_insert_with(|| HashMap::new());
            let partitions = topics.entry(topic).or_insert_with(|| HashMap::new());
            partitions.insert(partition, slices);
        }

        broker_partitioned
    }

    /// Make sure all topics in the buffer have corresponding metadata and if not, request
    async fn resolve_metadata(&self, cluster: &Cluster) {
        
        unimplemented!()
    }
}

type PartitionQueue = VecDeque<QueuedMessage>;

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
    Overflow,
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
    use std::time::Duration;
    //use async_std::task;

    struct P1 {}
    impl Partitioner<StringMessage> for P1 {
        fn partition(message: &StringMessage) -> u32 {
            5
        }
    }

    #[test]
    fn it_works() -> Result<()> {
        tokio_current_thread::block_on_all(async {
        //task::block_on(async {
            simple_logger::init_with_level(log::Level::Debug)?;
            let seed = "127.0.0.1:9092";
            let _topic = "test1";

            let producer = Producer::new::<P1>(&seed).await?;
            let mut producer = producer;
            /*for i in 1..100 {
                let msg = format!("i:{}", i);
                let msg = StringMessage {
                    key: i.to_string(),
                    value: msg,
                };
                producer.send(msg, "topic1".to_string()).await;
            }*/

            let msg = StringMessage {
                key: "".to_string(),
                value: "aaa".to_string(),
            };
            producer.send(msg, "test1".to_string()).await;

            async_std::task::sleep(Duration::from_secs(20)).await;

            /*producer
                .close()
                .await
                .expect("Failure when closing producer");
                */



            Ok(())
        })
    }

    /*
    #[test]
    fn mk_batch() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();
        let mut buffer = Buffer::new();
        buffer.ensure_topic(&protocol::TopicMetadata {
            topic: "topic 1".to_owned(),
            error_code: 0,
            partition_metadata: vec![
                protocol::PartitionMetadata {
                    error_code: 0,
                    partition: 0,
                    leader: 0,
                    replicas: 1,
                    isr: 1,
                },
                protocol::PartitionMetadata {
                    error_code: 0,
                    partition: 1,
                    leader: 0,
                    replicas: 1,
                    isr: 1,
                },
            ],
        });
        buffer.ensure_topic(&protocol::TopicMetadata {
            topic: "topic 2".to_owned(),
            error_code: 0,
            partition_metadata: vec![protocol::PartitionMetadata {
                error_code: 0,
                partition: 0,
                leader: 0,
                replicas: 1,
                isr: 1,
            }],
        });

        assert_eq!(
            buffer.add(&mk_msg(&"msg 1"), "topic 1".to_owned(), 0, 3),
            BufferingResult::Ok
        );
        assert_eq!(
            buffer.add(&mk_msg(&"msg 2"), "topic 1".to_owned(), 0, 3),
            BufferingResult::Ok
        );
        assert_eq!(
            buffer.add(&mk_msg(&"msg 3"), "topic 1".to_owned(), 1, 3),
            BufferingResult::Ok
        );
        assert_eq!(
            buffer.add(&mk_msg(&"msg 4"), "topic 2".to_owned(), 0, 3),
            BufferingResult::Ok
        );

        let rs = buffer.mk_requests();
        //debug!("Recordset: {:?}", rs);
    }

    fn mk_msg(msg: &str) -> StringMessage {
        StringMessage {
            key: "1".to_string(),
            value: msg.to_string(),
        }
    }
    */
}
