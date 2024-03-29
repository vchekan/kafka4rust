use crate::error::{KafkaError, BrokerResult, BrokerFailureSource};
use crate::murmur2a;
use crate::cluster::Cluster;
use crate::protocol::ErrorCode;
use crate::types::*;
use crate::utils;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing;
use tracing_attributes::instrument;
use crate::producer_buffer::Buffer;
use log::{debug, error};
use tokio::pin;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing_subscriber::fmt::time;
use crate::utils::TracedMessage;

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

pub trait Partitioner: Debug + Send + Sync {
    fn partition(&self, key: &[u8]) -> u32;
}

/// clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
pub struct Murmur2Partitioner {}
impl Partitioner for Murmur2Partitioner {
    fn partition(&self, key: &[u8]) -> u32 {
        murmur2a::hash32(key)
    }
}
impl Debug for Murmur2Partitioner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Murmur2Partitioner")
    }
}

#[derive(Debug)]
pub struct FixedPartitioner(pub u32);
impl Partitioner for FixedPartitioner {
    fn partition(&self, _key: &[u8]) -> u32 {
        self.0
    }
}

#[derive(Debug)]
enum BuffCmd {
    Flush,
    FlushAndClose,
}

// #[derive(Debug)]
// enum Msg {
//     Produce(QueuedMessage, String),
//     Flush(oneshot::Sender<BrokerResult<()>>),
// }

#[derive(Debug)]
pub struct ProducerBuilder<'a> {
    bootstrap: &'a str,
    hasher: Option<Box<dyn Partitioner>>,
    send_timeout: Option<Duration>,
}

impl<'a> ProducerBuilder<'a> {
    pub fn new(brokers: &'a str) -> Self {
        ProducerBuilder { bootstrap: brokers, hasher: None, send_timeout: None}
    }
    pub fn hasher(mut self, hasher: Box<dyn Partitioner>) -> Self { self.hasher = Some(hasher); self }
    pub fn send_timeout(self, timeout: Duration) -> Self { ProducerBuilder {send_timeout: Some(timeout), ..self} }
    
    /// If bootstrap address does not resolve, return error
    pub fn build(self) -> anyhow::Result<Producer> { Producer::new(self) }
}

// #[derive(Clone)]
// pub struct ProducerHandler {
//     tx: mpsc::Sender<TracedMessage<Msg>>,
// }

pub struct Producer {
    inner: Arc<Mutex<Inner>>
}

struct Inner {
    //rx: mpsc::Receiver<TracedMessage<Msg>>,
    bootstrap: String,
    buffer: Buffer,
    cluster: Cluster,
    partitioner: Box<dyn Partitioner>,
    /// Async response (ack/nack) to the caller
    // acks: Sender<Response>,
    /// Channel to the buffer
    // buffer_commands: Sender<BuffCmd>,
    //flush_loop_handle: tokio::task::JoinHandle<BrokerResult<()>>,
    send_timeout: Option<Duration>,
    /// Counter use to round-robin messages with null key
    null_key_partition_counter: u32,
    topic_partitions_count: HashMap<String,usize>,
}

impl Producer {
    pub fn builder(brokers: &str) -> ProducerBuilder {
        ProducerBuilder::new(brokers)
    }

    fn new(builder: ProducerBuilder) -> anyhow::Result<Producer> {
        let inner = Inner::new(builder)?;
        Ok(Producer {
            inner: Arc::new(Mutex::new(inner))
        })
    }
}

impl Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Producer: {{seed: '{}', partitioner: {:?}}}",
            self.bootstrap, self.partitioner
        )
    }
}

impl Inner {
    /// Resolves bootstrap addresses and creates producer.
    /// Broker connection and topology resolution will be started upon first `send` request.
    #[instrument(level = "debug", err)]
    fn new(builder: ProducerBuilder) -> anyhow::Result<Self> {
        // TODO: resolve names async and account for connect timeout

        let cluster = Cluster::new(builder.bootstrap.to_string(), builder.send_timeout);

        let producer = Inner {
            bootstrap: builder.bootstrap.to_string(),
            buffer: Buffer::new(),
            cluster,
            partitioner: builder.hasher.unwrap_or_else(|| Box::new(Murmur2Partitioner{})),
            // acks: ack_tx,
            // buffer_commands: buff_tx,
            send_timeout: builder.send_timeout,
            null_key_partition_counter: 0,
            topic_partitions_count: HashMap::new(),
        };

        Ok(producer)
    }

    async fn eval_loop(producer: Arc<Mutex<Inner>>) {
        let mut timer = tokio::time::sleep(publish_interval);

        loop {
            timer.await;

            let leader_map = producer.cluster.get_known_broker_map().await;
            let producer = producer.lock().expect("Failed to lock producer in eval_loop");
            producer.buffer.flush();

            timer.reset(Instant::now().add(publish_interval));
        }
    }

    #[instrument(err, skip(msg, self))]
    pub async fn send<M: ToMessage + 'static>(&mut self, msg: M, topic: &str) -> BrokerResult<()> {
        let msg = QueuedMessage {
            key: msg.key(),
            value: msg.value(),
            timestamp: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get timestamp")
                .as_millis() as u64,
        };

        // calculate partition by the key
        let partition = match &msg.key {
            Some(key) => self.partitioner.partition(key),
            None => {
                self.null_key_partition_counter += 1;
                self.null_key_partition_counter
            }
        };

        let partition_count = match self.topic_partitions_count.get(topic) {
            Some(count) => *count,
            None => {
                // TODO: no long-time blocking should exist
                let partition_count = self.cluster.get_or_fetch_partition_count(topic.to_string()).await?; //self.get_or_fetch_partition_count(&topic).await?;
                self.topic_partitions_count.insert(topic.to_string(), partition_count);
                partition_count
            }
        };

        let partition = partition % partition_count as u32;
        tracing::debug!("partition: {}", partition);

        let mut buffer = self.buffer.lock().expect("Failed to lock producer buffer");
        buffer.add(msg, topic, partition, partition_count as u32);

        Ok(())
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn flush(&mut self) -> BrokerResult<()> {
        todo!()
        // debug!("Flushing buffer before close");
        // // let mut cluster = self.cluster.write().await;
        // let res = self.buffer.flush(&mut self.acks, &self.cluster).await;
        // debug!("Flushing result: {:#?}", res);
        // res
    }

    /// Get topic's meta from cache or request from broker.
    /// Result will be cached.
    /// `self` is not passed as a parameter because it causes lifetime conflict in `send`.
    /// TODO: what is retry policy?
    /// TODO: what to do if failed for extended period of time?
    /// TODO: should it be in Cluster?
    // #[instrument(level = "debug", err, skip(cluster))]
    // async fn get_or_request_meta<'a>(
    //     cluster: &tokio::sync::RwLock<Cluster>,
    //     //topics_meta: &'a mut HashMap<String, ProducerTopicMetadata>,
    //     // buffer_meta: &mut HashMap<String, Vec<Option<BrokerId>>>,
    //     topic: &str,
    // ) -> Result<&'a mut ProducerTopicMetadata,BrokerFailureSource> {
    //     // `if let Some(meta) = self.topics_meta.get(topic)` does not work because of lifetimes,
    //     // so have to do 2 lookups :(
    //     if topics_meta.contains_key(topic) {
    //         let topic_meta = topics_meta;
    //         let meta = topic_meta.get_mut(topic).unwrap();
    //         return Ok(meta);
    //     }
    //
    //     // Did not have meta. Fetch and cache.
    //     // TODO: instead of loop use recovery policy
    //     // TODO: will wait for *all* partitions to become available. Could progress on what's up for now
    //     // and await only for failed ones?
    //     // loop {
    //         debug!("Fetching topic meta from server");
    //         let meta = cluster.write().await.fetch_topic_meta_and_update(&[topic]).await?;
    //         let meta = ProducerTopicMetadata {
    //             protocol_metadata: meta,
    //             null_key_partition_counter: random(),
    //         };
    //         let topic_metadata = &meta.protocol_metadata.topics[0];
    //
    //     meta.protocol_metadata.topics[0].error_code.as_result()?;
    //
    //         // match meta.protocol_metadata.topics[0].error_code.as_result() {
    //         //     Err(e) if e.is_retriable() => {
    //         //         info!("Retriable error {}", e);
    //         //         tokio::time::sleep(Duration::from_millis(300))
    //         //             .instrument(tracing::info_span!("Retry sleep"))
    //         //             .await;
    //         //         //continue;
    //         //         return Err(BrokerFailureSource::KafkaErrorCode(e))
    //         //     }
    //         //     Err(e) => return Err(e.into()),
    //         //     _ => {}
    //         // }
    //
    //         if topic_metadata
    //             .partition_metadata
    //             .iter()
    //             .all(|m| m.error_code == ErrorCode::None)
    //         {
    //             let mut partition_leader_map: Vec<Option<i32>> =
    //                 vec![None; topic_metadata.partition_metadata.len()];
    //             for partition_meta in &topic_metadata.partition_metadata {
    //                 // TODO: got exception once: index 5 is out of range of 5. How come? Switch to map?
    //                 partition_leader_map[partition_meta.partition as usize] = Some(partition_meta.leader);
    //             }
    //             assert!(partition_leader_map.iter().all(|l| l.is_some()));
    //             buffer_meta.insert(topic.to_string(), partition_leader_map);
    //             topics_meta.insert(topic.to_string(), meta);
    //
    //             let meta = topics_meta.get_mut(topic).unwrap();
    //             return Ok(meta);
    //         } else {
    //             for partition_meta in topic_metadata
    //                 .partition_metadata
    //                 .iter()
    //                 .filter(|m| m.error_code != ErrorCode::None)
    //             {
    //                 event!(target: "get_or_request_meta", tracing::Level::ERROR, error_code = ?partition_meta.error_code, partition = ?partition_meta.partition);
    //             }
    //             tokio::time::sleep(Duration::from_secs(3))
    //                 .instrument(tracing::info_span!("Retry sleep"))
    //                 .await;
    //             // TODO: check either error is recoverable
    //             //continue;
    //             return BrokerFailureSource::KafkaErrorCode()
    //         }
    //     // }
    // }

    #[instrument(level="debug", err, skip(self))]
    pub async fn close(self) -> anyhow::Result<()> {
        todo!()
        // debug!("Closing producer...");
        // self.buffer_commands.send(BuffCmd::FlushAndClose).await?;
        // debug!("Sent BuffCmd::FlushAndClose, waiting for loop exit");
        // self.flush_loop_handle.await.context("producer closing")??;
        // debug!("Producer closed");
        // Ok(())
    }
}

// #[instrument(level = "debug")]
// async fn flushing_loop(mut buff_rx: Receiver<BuffCmd>, buffer: &mut Buffer, ack_tx: Sender<Response>, cluster: ClusterHandler, send_timeout: Duration) -> BrokerResult<()> {
//     let mut complete = false;
//     loop {
//         //     // TODO: check time since last flush
//         //     // TODO: configure flush time
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

//         debug!("Waiting for buffer locks");
//         // TODO: is buffer locked worst-case until flush timeout? Does it mean no append can happen?
//         //let mut buffer2 = buffer.lock().await;
//         // TODO: handle result
//         debug!("Flushing with {:?} send_timeout", send_timeout);
//         // TODO: use Duration::MAX when stabilized

//         // let res = buffer2.flush(&ack_tx2, &cluster).await;
//         let res: BrokerResult<()> = match timeout(send_timeout, buffer.flush(&ack_tx, &cluster)).await {
//             Err(_) => {
//                 tracing::warn!("Flushing timeout");
//                 Err(BrokerFailureSource::Timeout)
//             },
//             Ok(Err(e)) => {
//                 error!("Failed to flush buffer. {:?}", e);
//                 Err(e)
//             },
//             Ok(Ok(_)) => {
//                 tracing::trace!("Flush Ok");
//                 Ok(())
//             }
//         };

//         if complete {
//             debug!("Buffer flush loop quit");
//             return  res;
//             // return Ok(())
//         }
//     };
// }

enum EvalLoop {

}

impl Future for EvalLoop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
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

#[cfg(test)]
mod tests {
    use futures::future::Fuse;
    use super::*;
    use log::LevelFilter;
    use crate::init_tracer;
    use tracing;
    use tracing::{Instrument, span, Level};

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        simple_logger::SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
        let _trace = init_tracer("producer-test")?;

        let mut producer = ProducerBuilder::new("localhost").build()?;
        //let producer2 = producer.clone();
        let producer = tokio::spawn(async move {
            for i in 0..10_000 {
                producer.send(format!("msg {}", i), "test1").await?;
            }
            BrokerResult::Ok(producer)
        }).instrument(span!(Level::INFO, "send loop task")).await??;

        producer.close().await?;

        Ok(())
    }
}