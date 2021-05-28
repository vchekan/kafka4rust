use crate::cluster::Cluster;
use crate::error::{KafkaError, InternalError};
use crate::murmur2a;
use crate::protocol;
use crate::protocol::{ErrorCode, ProduceResponse3, PartitionResponse, ProduceResponse};
use crate::types::*;
use crate::utils;
use crate::error::Result;
use async_std::sync::Mutex;
use rand::random;
use std::collections::{HashMap, VecDeque, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::timeout;
use async_stream;
use tracing::{self, event, Level};
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use tracing_subscriber::fmt::format::debug_fn;
use tokio::task::JoinHandle;
use anyhow::Context;
use futures::stream::{self, StreamExt};

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
/// Q: how to wait for Producer to complete? Explicit `Producer::flush()`? Should individual message
/// await be implemented?
/// A:
///
/// Q: How to report produce errors?
/// A: ???
///

// TODO: is `Send` needed? Can we convert to QueuedMessage before crossing thread boundaries?
// TODO: maybe can replace with &[u8] also?
pub trait ToMessage: Send {
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Vec<u8>;
}

pub trait Partitioner: Debug + Send {
    fn partition(&self, key: &[u8]) -> u32;
}

// clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
pub struct Murmur2Partitioner {}
impl Partitioner for Murmur2Partitioner {
    fn partition(&self, key: &[u8]) -> u32 {
        murmur2a::hash32(key)
    }
}

#[derive(Debug)]
pub struct FixedPartitioner(pub u32);
impl Partitioner for FixedPartitioner {
    fn partition(&self, _key: &[u8]) -> u32 {
        self.0
    }
}

impl Debug for Murmur2Partitioner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Murmur2Partitioner")
    }
}

#[derive(Debug)]
enum BuffCmd {
    Flush,
    FlushAndClose,
}

#[derive(Debug)]
pub struct ProducerBuilder<'a> {
    brokers: &'a str,
    hasher: Option<Box<dyn Partitioner>>,
    send_timeout: Option<Duration>,
}

impl<'a> ProducerBuilder<'a> {
    pub fn new(brokers: &'a str) -> Self {
        ProducerBuilder { brokers, hasher: None, send_timeout: None}
    }
    pub fn hasher(mut self, hasher: Box<dyn Partitioner>) -> Self { self.hasher = Some(hasher); self }
    pub fn send_timeout(self, timeout: Duration) -> Self { ProducerBuilder {send_timeout: Some(timeout), ..self} }
    pub fn start(self) -> anyhow::Result<(Producer,Receiver<Response>)> { Ok(Producer::new(self)?) }
}

pub struct Producer {
    bootstrap: String,
    buffer: Arc<Mutex<Buffer>>,
    cluster: Arc<tokio::sync::RwLock<Cluster>>,
    partitioner: Box<dyn Partitioner>,
    // TODO: is kafka topic case-sensitive?
    topics_meta: HashMap<String, ProducerTopicMetadata>,
    acks: Sender<Response>,
    buffer_commands: Sender<BuffCmd>,
    flush_loop_handle: tokio::task::JoinHandle<Result<()>>,
    send_timeout: Option<Duration>,
}

impl Debug for Producer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Producer: {{seed: '{}', partitioner: {:?}}}",
            self.bootstrap, self.partitioner
        )
    }
}

/*
impl Producer {
    /// Synchronously resolve bootstrap servers DNS addresses and start background worker process
    /// to connect and flush data buffer periodically.
    /// Note, that actual connection to the broker is not happenings until first message is sent.
    /// Default MURMUR2 hasher is used (the same as Java).
    #[instrument(level = "debug")]
    fn new(seed: &str, ) -> Result<(Self, Receiver<Response>), KafkaError> {
        Producer::with_hasher(seed, Box::new(Murmur2Partitioner {}))
    }
}
*/

impl Producer {
    #[instrument(level = "debug", err)]
    pub fn new(builder: ProducerBuilder) -> anyhow::Result<(Self, Receiver<Response>)> {
        // TODO: resolve names async and account for connect timeout
        let seed_list = utils::resolve_addr(builder.brokers);
        if seed_list.is_empty() {
            return Err(KafkaError::NoBrokerAvailable(format!(
                "No address can be resolved: {}",
                builder.brokers
            )).into());
        }
        let cluster = Arc::new(tokio::sync::RwLock::new(Cluster::new(seed_list, builder.send_timeout)));
        // let cluster2 = cluster.clone();
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(1000);
        // let mut ack_tx2 = ack_tx.clone();
        let (buff_tx, mut buff_rx) = channel::<BuffCmd>(2);

        let buffer = Arc::new(Mutex::new(Buffer::new(cluster.clone())));
        // let buffer2 = buffer.clone();
        // TODO: default flush timeout is not very scientific. Think either whole flush should have timeout or its internal parts
        let send_timeout = builder.send_timeout.unwrap_or(Duration::from_secs(30));

        // TODO: wait in `close` for loop to end
        let flush_loop_handle: JoinHandle<Result<()>> = tokio::spawn(flushing_loop(buff_rx, buffer.clone(), ack_tx.clone(), cluster.clone(), send_timeout));
        // let flush_loop_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        //     // let mut topics_in_resolution = HashSet::<String>::new();
        //     // let mut buff_rx = buff_rx;
        //     let mut complete = false;
        //     loop {
        //         // TODO: check time since last flush
        //         // TODO: configure flush time
        //         tokio::select! {
        //             _ = tokio::time::sleep(Duration::from_secs(5)) => { debug!("Buffer timer"); }
        //             cmd = buff_rx.recv() => {
        //                 match cmd {
        //                     Some(BuffCmd::Flush) => {
        //                         debug!("Flushing buffer");
        //                     }
        //                     Some(BuffCmd::FlushAndClose) => {
        //                         debug!("Buffer flush before closing");
        //                         complete = true;
        //                     }
        //                     None => {
        //                         debug!("Producer closed. Exiting buffer flush loop");
        //                         complete = true;
        //                     }
        //                 }
        //             }
        //         }
        //
        //         debug!("Waiting for buffer locks");
        //         // TODO: is buffer locked worst-case until flush timeout? Does it mean no append can happen?
        //         let mut buffer2 = buffer.lock().await;
        //         // TODO: handle result
        //         debug!("Flushing with {:?} send_timeout", send_timeout);
        //         // TODO: use Duration::MAX when stabilized
        //
        //         let res = buffer2.flush(&ack_tx2, &cluster).await;
        //         // let res: Result<()> = match timeout(send_timeout, buffer2.flush(&mut ack_tx2, &mut cluster)).await {
        //         //     Err(_) => {
        //         //         tracing::warn!("Flushing timeout");
        //         //         Err(InternalError::Timeout)
        //         //     },
        //         //     Ok(Err(e)) => {
        //         //         error!("Failed to flush buffer. {:?}", e);
        //         //         Err(e)
        //         //     },
        //         //     Ok(Ok(_)) => {
        //         //         tracing::trace!("Flush Ok");
        //         //         Ok(())
        //         //     }
        //         // };
        //
        //         if complete {
        //             debug!("Buffer flush loop quit");
        //             // return  res;
        //             return Ok(())
        //         }
        //     };
        // }.instrument(tracing::info_span!("flush_loop")));

        let producer = Producer {
            bootstrap: builder.brokers.to_string(),
            buffer,
            cluster,
            partitioner: builder.hasher.unwrap_or_else(|| Box::new(Murmur2Partitioner{})),
            topics_meta: HashMap::new(),
            acks: ack_tx,
            buffer_commands: buff_tx,
            flush_loop_handle,
            send_timeout: builder.send_timeout
        };

        Ok((producer, ack_rx))
    }

    #[instrument(level = "debug", err, skip(msg, self))]
    pub async fn send<M: ToMessage + 'static>(&mut self, msg: M, topic: &str) -> Result<(),InternalError> {
        let mut buffer = self.buffer.lock().await;
        // TODO: share metadata cache between self and Buffer
        let meta = Self::get_or_request_meta(
            &self.cluster,
            &mut self.topics_meta,
            &mut buffer.meta_cache,
            topic,
        )
        .await?;
        assert_eq!(1, meta.protocol_metadata.topics.len());
        assert_eq!(topic, meta.protocol_metadata.topics[0].topic);
        let topic_meta = meta
            .protocol_metadata
            .topics
            .get(0)
            .expect("Expect exacly 1 topic configured");

        let partitions_count = topic_meta.partition_metadata.len() as u32;
        let partition = match msg.key() {
            Some(key) if !key.is_empty() => self.partitioner.partition(&key),
            _ => {
                meta.null_key_partition_counter += 1;
                meta.null_key_partition_counter
            }
        };
        let partition = partition % partitions_count;

        // TODO: would it be possible to keep reference to the message data instead of cloning? Would it be possible to do both?
        let msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            timestamp: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get timestamp")
                .as_millis() as u64,
        };
        match buffer.add(msg, topic.to_string(), partition, &topic_meta) {
            BufferingResult::Ok => Ok(()),
            BufferingResult::Overflow => {
                // TODO:
                unimplemented!()
            }
        }
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn flush(&mut self) -> Result<(),InternalError> {
        debug!("Flushing buffer before close");
        let mut buffer = self.buffer.lock().await;
        // let mut cluster = self.cluster.write().await;
        let res = buffer.flush(&mut self.acks, &self.cluster).await;
        debug!("Flushing result: {:#?}", res);
        res
    }

    /// Get topic's meta from cache or request from broker.
    /// Result will be cached.
    /// `self` is not passed as a prarameter because it causes lifetime conflict in `send`.
    /// TODO: what is retry policy?
    /// TODO: what to do if failed for extended period of time?
    #[instrument(level = "debug", err, skip(cluster, topics_meta, buffer_meta))]
    async fn get_or_request_meta<'a>(
        cluster: &tokio::sync::RwLock<Cluster>,
        topics_meta: &'a mut HashMap<String, ProducerTopicMetadata>,
        buffer_meta: &mut HashMap<String, Vec<Option<BrokerId>>>,
        topic: &str,
    ) -> Result<&'a mut ProducerTopicMetadata,InternalError> {
        // `if let Some(meta) = self.topics_meta.get(topic)` does not work because of lifetimes,
        // so have to do 2 lookups :(
        if topics_meta.contains_key(topic) {
            let topic_meta = topics_meta;
            let meta = topic_meta.get_mut(topic).unwrap();
            return Ok(meta);
        }

        // Did not have meta. Fetch and cache.
        // TODO: unstead of loop use recovery policy
        // TODO: will wait for *all* partitions to become available. Could progress on what's up for now
        // and await only for failed ones?
        loop {
            debug!("Fetching topic meta from server");
            let meta = cluster.write().await.fetch_topic_meta_and_update(&[topic]).await?;
            let meta = ProducerTopicMetadata {
                protocol_metadata: meta,
                null_key_partition_counter: random(),
            };
            let topic_metadata = &meta.protocol_metadata.topics[0];

            match meta.protocol_metadata.topics[0].error_code.as_result() {
                Err(e) if e.is_retriable() => {
                    info!("Retriable error {}", e);
                    tokio::time::sleep(Duration::from_millis(300))
                        .instrument(tracing::info_span!("Retry sleep"))
                        .await;
                    continue;
                }
                Err(e) => return Err(e.into()),
                _ => {}
            }

            if topic_metadata
                .partition_metadata
                .iter()
                .all(|m| m.error_code == ErrorCode::None)
            {
                let mut partition_leader_map: Vec<Option<i32>> =
                    vec![None; topic_metadata.partition_metadata.len()];
                for partition_meta in &topic_metadata.partition_metadata {
                    // TODO: got exception once: index 5 is out of range of 5. How come? Switch to map?
                    partition_leader_map[partition_meta.partition as usize] = Some(partition_meta.leader);
                }
                assert!(partition_leader_map.iter().all(|l| l.is_some()));
                buffer_meta.insert(topic.to_string(), partition_leader_map);
                topics_meta.insert(topic.to_string(), meta);

                let meta = topics_meta.get_mut(topic).unwrap();
                return Ok(meta);
            } else {
                for partition_meta in topic_metadata
                    .partition_metadata
                    .iter()
                    .filter(|m| m.error_code != ErrorCode::None)
                {
                    event!(target: "get_or_request_meta", tracing::Level::ERROR, error_code = ?partition_meta.error_code, partition = ?partition_meta.partition);
                }
                tokio::time::sleep(Duration::from_secs(3))
                    .instrument(tracing::info_span!("Retry sleep"))
                    .await;
                // TODO: check either error is recoverable
                continue;
            }
        }
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn close(self) -> anyhow::Result<()> {
        debug!("Closing producer...");
        self.buffer_commands.send(BuffCmd::FlushAndClose).await?;
        debug!("Sent BuffCmd::FlushAndClose, waiting for loop exit");
        self.flush_loop_handle.await.context("producer closing")??;
        debug!("Producer closed");
        Ok(())
    }
}

#[instrument(level = "debug")]
async fn flushing_loop(mut buff_rx: Receiver<BuffCmd>, buffer: Arc<Mutex<Buffer>>, ack_tx2: Sender<Response>, cluster: Arc<RwLock<Cluster>>, send_timeout: Duration) -> Result<()> {
    let mut complete = false;
    loop {
        //     // TODO: check time since last flush
        //     // TODO: configure flush time
        tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => { debug!("Buffer timer"); }
                    cmd = buff_rx.recv() => {
                        match cmd {
                            Some(BuffCmd::Flush) => {
                                debug!("Flushing buffer");
                            }
                            Some(BuffCmd::FlushAndClose) => {
                                debug!("Buffer flush before closing");
                                complete = true;
                            }
                            None => {
                                debug!("Producer closed. Exiting buffer flush loop");
                                complete = true;
                            }
                        }
                    }
                }

        debug!("Waiting for buffer locks");
        // TODO: is buffer locked worst-case until flush timeout? Does it mean no append can happen?
        let mut buffer2 = buffer.lock().await;
        // TODO: handle result
        debug!("Flushing with {:?} send_timeout", send_timeout);
        // TODO: use Duration::MAX when stabilized

        // let res = buffer2.flush(&ack_tx2, &cluster).await;
        let res: Result<()> = match timeout(send_timeout, buffer2.flush(&ack_tx2, &cluster)).await {
            Err(_) => {
                tracing::warn!("Flushing timeout");
                Err(InternalError::Timeout)
            },
            Ok(Err(e)) => {
                error!("Failed to flush buffer. {:?}", e);
                Err(e)
            },
            Ok(Ok(_)) => {
                tracing::trace!("Flush Ok");
                Ok(())
            }
        };

        if complete {
            debug!("Buffer flush loop quit");
            return  res;
            // return Ok(())
        }
    };
}

struct ProducerTopicMetadata {
    protocol_metadata: protocol::MetadataResponse0,
    null_key_partition_counter: u32,
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
/// ProduceMessage --| broker_id 1 | topic 1 --|partition 0; recordset
///                  |                         |partition 1; recordset
///                  |
///                  | broker_id 2 | topic 2 --| partition 0; recordset
///                                |
///                  |             | topic 3 --| partition 0; recordset
///
#[derive(Debug)]
struct Buffer {
    topic_queues: HashMap<String, Vec<PartitionQueue>>,
    bytes: u32,
    size_limit: u32,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    meta_cache: HashMap<String, Vec<Option<BrokerId>>>,
    cluster: Arc<RwLock<Cluster>>,
}

impl Buffer {
    fn new(cluster: Arc<RwLock<Cluster>>) -> Self {
        Buffer {
            topic_queues: HashMap::new(),
            bytes: 0,
            // TODO: make configurable
            size_limit: 100 * 1024 * 1024,
            meta_cache: HashMap::new(),
            cluster,
        }
    }

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    fn add(
        &mut self,
        msg: QueuedMessage,
        topic: String,
        partition: u32,
        meta: &protocol::TopicMetadata,
    ) -> BufferingResult {
        if self.bytes + msg.value.len() as u32 > self.size_limit {
            debug!("Overflow");
            return BufferingResult::Overflow;
        }

        let partitions_count = meta.partition_metadata.len();
        let partitions = self.topic_queues.entry(topic).or_insert_with(|| {
            (0..partitions_count)
                .map(|_| PartitionQueue::default())
                .collect()
        });

        self.bytes += (msg.value.len() + msg.key.as_ref().map(|k| k.len()).unwrap_or(0)) as u32;

        partitions[partition as usize].queue.push_back(msg);
        BufferingResult::Ok
    }

    /// TODO: rust BC to become smarter. Parameter `cluster` is member of `self` but I have to pass it separately because borrow checker
    /// complains about `self` being borrowed 2 times mutably.
    //#[instrument(level = "debug", err, skip(self, acks, cluster))]
    async fn flush(&mut self, acks: &Sender<Response>, cluster: &Arc<RwLock<Cluster>>) -> Result<()> {
        let broker_partitioned = self.group_queue_by_leader();

        // Need to collect partition success or failure to adjust buffer,
        // and re-issue fetching topic metadata.
        // If contacting broker failed, then re-fetch meta for every topic in this broker.
        let mut responses: Vec<(BrokerId, Result<ProduceResponse3>)> = vec![];

        // TODO: send to all leaders in parallel. Use `Stream`?
        for (&leader, data) in &broker_partitioned {
            // let res: Vec<_> = broker_partitioned.into_iter().map(|(leader, data)| tokio::spawn(async move {
            // let responses = async_stream::stream! {
            // for (&leader, data) in &broker_partitioned {
            // TODO: stream requires `static lifetime but we have messages as references inside buffer...
            //let mut responses = stream::iter(broker_partitioned).map(|(leader, data)| async move {
            let request = protocol::ProduceRequest3 {
                transactional_id: None,
                acks: 1,
                timeout: 1500,
                topic_data: &data,
            };


            // TODO: upon failure to connect to broker, reset connection and refresh metadata
            // TODO: check locks scope to be minimal possible
            let cluster = cluster.read().await;
            let broker = match cluster.broker_get_no_connect(leader).await {
                Ok(broker) => broker,
                Err(e) => /*return*/ {
                    // cluster.start_resolving_topics(data.keys().map(|t| *t)).await?;
                    //yield (leader, Err(e));
                    // return Err(e);
                    // self.mark_leader_down(leader);
                    responses.push((leader, Err(e)));
                    continue;
                }
            };

            let cmd_buf = broker.mk_request(request);
            debug!("Sending Produce");
            // Any failure to
            let response = broker
                .send_request2::<protocol::ProduceResponse3>(cmd_buf)
                .await;
            // TODO: throttle_time

            let response = match response {
                Err(e) => {
                    // Send was not successful.
                    // Reset broker. This will cause fresh metadata fetch in the next round
                    // cluster.reset_broker(leader);
                    // TODO: `continue` with other partitions instead of `return`
                    /*return*/
                    // yield (leader, Err(e));
                    // return Err(e);
                    // self.mark_leader_down(leader);
                    responses.push((leader, Err(e)));
                    continue;
                }
                Ok(response) => response
            };

            // Container to keep successful topic/partitions after the call loop. We can not modify queue
            // in the loop because loop keeps immutable reference to `self`.
            // Topic -> partition
            // let mut messages_to_discard = HashMap::<String, Vec<u32>>::new();

            debug!("Got Produce response: {:?}", response);
            for topic_resp in &response.responses {
                for partition_resp in &topic_resp.partition_responses {
                    let ack = if partition_resp.error_code.is_ok() {
                        debug!("Ok sent partition {} to broker {:?}", partition_resp.partition, broker);

                        // messages_to_discard
                        //     .entry(topic_resp.topic.clone())
                        //     .or_default()
                        //     .push(partition_resp.partition as u32);

                        Response::Ack {
                            partition: partition_resp.partition as u32,
                            offset: partition_resp.base_offset as u64,
                            error: partition_resp.error_code,
                        }
                    } else {
                        error!(
                            "Produce error. Topic {} partition {} error {:?}",
                            topic_resp.topic, partition_resp.partition, partition_resp.error_code
                        );
                        tracing::event!(tracing::Level::ERROR, ?leader, ?topic_resp.topic, partition = ?partition_resp.partition, error_code = ?partition_resp.error_code, "Produce error");
                        Response::Nack {
                            partition: partition_resp.partition as u32,
                            error: partition_resp.error_code,
                        }
                    };

                    // TODO: send ack
                }
            }
            responses.push((leader, Ok(response)));
        }

        // `broker_partitioned` keeps `&mut self` reference which makes it impossible to mutate `self.topic_queues`,
        // so shadow it with only parts we need
        // TODO: this feels clumsy. When no error happen, it cause redundant `clone()`. Think better borrow design.
        let broker_partitioned: HashMap<BrokerId,Vec<String>> = broker_partitioned.into_iter()
            .map(|e| (e.0, e.1.keys().into_iter().map(|k| (*k).clone()).collect())).collect();


        // Discard messages which were sent successfully
        for (leader, response) in responses {
            match response {
                Ok(response) => {
                    for response in response.responses {
                        let topic = response.topic;
                        for response in response.partition_responses {
                            let queues = self.topic_queues.get_mut(&topic).expect("Topic not found");
                            let partition = response.partition;
                            //for partition in response.responses {
                            let queue = &mut queues[partition as usize];
                            queue.queue.drain(..queue.sending as usize);
                            queue.sending = 0;
                            let remaining = queue.queue.len();
                            event!(
                                    Level::DEBUG,
                                    ?topic,
                                    ?partition,
                                    ?remaining,
                                    "Truncated queue");
                            //}
                        }
                    }
                }
                // leader failed and topology of all topics served by this leader need to be refreshed
                Err(e) => {
                    let topics = broker_partitioned.get(&leader)
                        // Leader is put into `responses` from `broker_partitioned`, so no chance for it to be missing
                        .unwrap();

                    for topic in topics {
                        self.mark_leader_down(leader);
                        self.cluster.write().await.start_resolving_topic((*topic).clone()).await?;
                        info!("Marked leader down: {}", leader);
                    }
                }
            }
        }

        Ok(())
    }

    /// Scan message buffer and group messages by the leader and update `sending` counter.
    /// Two queues because that's how dequeue works.
    /// Returns leader->topic->partition->messages
    fn group_queue_by_leader(
        &mut self,
    ) -> HashMap<BrokerId, HashMap<&String, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>>>
    {
        // TODO: implement FIFO and request size bound algorithm

        // leader->topic->partition->recordset[]
        let mut broker_partitioned = HashMap::<
            BrokerId,
            HashMap<&String, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>>,
        >::new();

        for (topic, partitioned_queue) in &mut self.topic_queues {
            for (partition, queue) in partitioned_queue
                .iter_mut()
                .enumerate()
                // Only non-empty queues
                .filter(|(_, q)| !q.queue.is_empty())
            {
                let partition = partition as u32;
                let leader = self
                    .meta_cache
                    .get(topic)
                    .expect("Topic metadata is expected")
                    .get(partition as usize)
                    .expect("Corrupt topic metadata partition info: can not find leader for partition");

                let leader = match leader {
                    Some(leader) => leader,
                    None => {
                        trace!("Leader for partition {} is not known yet, skipping", partition);
                        continue;
                    }
                };

                // TODO: limit message size
                queue.sending = queue.queue.len().try_into().unwrap();

                let topics = broker_partitioned
                    .entry(*leader)
                    .or_insert_with(HashMap::new);
                // TODO use raw entries
                let partitions = topics.entry(topic).or_insert_with(HashMap::new);
                partitions.insert(partition, queue.queue.as_slices());
            }
        }

        broker_partitioned
    }

    fn mark_leader_down(&mut self, leader: BrokerId) {
        for (_topic, partitions) in &mut self.meta_cache {
            for leader in partitions {
                *leader = None
            }
        }
    }
}

#[derive(Debug, Default)]
struct PartitionQueue {
    queue: VecDeque<QueuedMessage>,
    // How many message are being sent
    sending: u32,
}

#[derive(Debug)]
pub enum Response {
    Ack {
        partition: u32,
        offset: u64,
        error: ErrorCode,
    },
    // TODO:
    /*TempNack {
        partition: u32,
        error: ErrorCode,
    },*/
    Nack {
        partition: u32,
        error: ErrorCode,
    },
}

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

pub enum TimestampType {
    Create = 0,
    LogAppend = 1,
}

// TODO: research rust `impl specialization` and create a single-value impl for `T: AsRef<[u8]>`
// default impl<T> ToMessage for T
//     where T: AsRef<[u8]> + Send
// {
//     fn key(&self) -> Option<Vec<u8>> {
//         None
//     }
//     fn value(&self) -> Vec<u8> {
//         Vec::from(self.as_ref().as_bytes())
//     }
// }

// TODO: see if Send can be eliminated by serializing in the caller's thread
impl<K, V> ToMessage for (Option<K>, V)
where
    K: Send + AsRef<[u8]>,
    V: Send + AsRef<[u8]>,
{
    fn key(&self) -> Option<Vec<u8>> { self.0.as_ref().map(|k| k.as_ref().to_vec()) }
    fn value(&self) -> Vec<u8> { self.1.as_ref().to_vec() }
}

// impl<K, V> ToMessage for (K, V)
//     where
//         K: Send + AsRef<str>,
//         V: Send + AsRef<str>,
// {
//     fn key(&self) -> Option<Vec<u8>> { Some(Vec::from(self.0.as_ref().as_bytes())) }
//     fn value(&self) -> Vec<u8> { Vec::from(self.1.as_ref().as_bytes()) }
// }

impl ToMessage for &str {
    fn key(&self) -> Option<Vec<u8>> { None }
    fn value(&self) -> Vec<u8> { Vec::from(self.as_bytes()) }
}

impl ToMessage for String {
    fn key(&self) -> Option<Vec<u8>> { None }
    fn value(&self) -> Vec<u8> { Vec::from(self.as_bytes()) }
}

impl ToMessage for &[u8] {
    fn key(&self) -> Option<Vec<u8>> { None }
    fn value(&self) -> Vec<u8> { Vec::from(*self) }
}

// impl ToMessage for (&str, String) {
//     fn key(&self) -> Option<Vec<u8>> { None }
//     fn value(&self) -> Vec<u8> { Vec::from(self) }
// }
