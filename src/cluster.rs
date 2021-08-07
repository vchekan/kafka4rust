//! Design questions
//! Q: What is communication and ownership between Cluster and TopicResolver?
//! A: channels
//!
//! Q: How TopicResolver communicates with Connection?
//! A: Connections is a shared resource, used by both, Topic Resolver and by Producer. Topic Resolver
//! can send requests either to connection or through Producer. If directly then it must be aware of
//! cluster configuration, i.e. list of servers. If through Producer then Producer should have
//! response routing capabilities. This will force us to reimplement it in Consumer. Another design
//! is metadata message bus.
//!
//! Q: should topic resolution be a separate component or part of Cluster?
//! A: list of unresolved topics is owned by resolver spawn, thus it can not belong to Cluster.
//!
//! Q: should connect upon initialization or first request?
//! A: initialization because it will improve startup time. But on the other hand,
//! if app waits for connection before sending, we achieved nothing. Also, it is better to connect
//! upon initialization for sake of Fail Fast principle.
//!
//! Q: Should `Connection::request()` take `&self` or `&mut self`?
//! A: Problem with reading answer. If 2 requests were send immediately one after another, Kafka
//! will preserve order. But do we have guaranteed order of execution of 2 tasks reading responses?
//! I don't think so. So we need correlation_id based responses.
//!
//! Q: Should close seed connection after resolve 1st member of the cluster? Or use for metadata
//! querying? Which connections should be used for metadata query, because long polling problem?
//! A: ???

use crate::error::{BrokerResult, InternalError, BrokerFailureSource};
use crate::futures::{repeat_with_timeout, RepeatResult};
use crate::protocol;
use crate::types::*;
use crate::utils::resolve_addr;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::time::Duration;
use tracing_attributes::instrument;
use crate::resolver;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Sender, Receiver};
use crate::connection::ConnectionHandle;
use std::fmt::{Debug, Formatter};
use tracing::event;
use crate::protocol::{ErrorCode, MetadataResponse0};
use log::{debug, info, warn, error};
use crate::resolver::ResolverHandle;

type LeaderMap = Vec<(BrokerId, Vec<(String, Vec<Partition>)>)>;

#[derive(Clone)]
pub struct ClusterHandler {
    tx: mpsc::Sender<Msg>,
}

pub enum Msg {
    GetLeaderMap(Vec<String>, oneshot::Sender<LeaderMap>),
}

struct Cluster {
    bootstrap: Vec<SocketAddr>,
    operation_timeout: Duration,
    rx: mpsc::Receiver<Msg>,
    resolver: ResolverHandle,
    brokers_maps: BrokersMaps,
}

#[derive(Default)]
struct BrokersMaps {
    pub broker_id_map: HashMap<BrokerId, ConnectionHandle>,
    pub broker_addr_map: HashMap<BrokerId, SocketAddr>,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    meta_cache: HashMap<String, Vec<Option<BrokerId>>>,
    topics_meta: HashMap<String, TopicMeta>,
}

impl ClusterHandler {
    pub fn new(bootstrap: Vec<SocketAddr>, timeout: Option<Duration>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let (listener_tx, listener_rx) = mpsc::channel::<BrokerResult<protocol::TopicMetadata>>(1);
        //let resolver = ResolverHandle::new(bootstrap, listener_tx);
        let cluster = Cluster::new(bootstrap, rx, timeout);
        tokio::spawn(run(cluster));
        ClusterHandler { tx }
    }

    /// If broker is known, return it in the map, otherwise trigger topic resolution and ignore this
    /// topic for now.
    pub async fn resolve(&self, topics: Vec<String>) -> LeaderMap {
        let (req, resp) = oneshot::channel::<LeaderMap>();
        let _ = self.tx.send(Msg::GetLeaderMap(topics, req)).await;
        match resp.await {
            Ok(resp) => resp,
            Err(e) => {
                panic!("Cluster: resolver channel failed: {}", e)
            }
        }
    }
}

async fn run(mut cluster: Cluster) {
    debug!("Cluster loop starting");
    loop {
        tokio::select! {
            Some(msg) = cluster.rx.recv() => {
                cluster.handle(msg).await;
            },
            Ok(msg) = cluster.resolver.listener.recv() => {
                debug!("Got resolver message");
                cluster.update_brokers_map(&msg);
            },
            else => { break; }
        }
    }
    while let Some(msg) = cluster.rx.recv().await {
        cluster.handle(msg).await;
    }
    debug!("Cluster loop complete, exiting");
}

impl Debug for Cluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
        .field("bootstrap", &self.bootstrap)
        .field("operation_timeout", &self.operation_timeout)
        // TODO: need to block maps to get their string
        //.field("brokers_map", &self.brokers_maps)
        .finish()
    }
}

impl Cluster {
    #[instrument(err)]
    pub fn with_bootstrap(bootstrap: &str, rx: mpsc::Receiver<Msg>, timeout: Option<Duration>) -> anyhow::Result<Self> {
        let bootstrap = resolve_addr(bootstrap);
        Ok(Self::new(bootstrap, rx, timeout))
    }

    /// Connect to at least one broker successfully .
    pub fn new(bootstrap: Vec<SocketAddr>, rx: mpsc::Receiver<Msg>, operation_timeout: Option<Duration>) -> Self {
        /*let connect_futures = bootstrap.iter()
            // TODO: log bad addresses which did not parse
            //.filter_map(|addr| addr.parse().ok())
            .map(|a| Broker::connect(a.clone()));

        // TODO: use `fold` to preserve and report last error if no result

        let mut resolved =
            FuturesUnordered::from_iter(connect_futures).filter_map(|f| ready(f.ok()));
        let broker = match resolved.next().await {
            Some(broker) => broker,
            // TODO: failure
            None => {
                return Err(Error::DnsFailed(format!("Cluster: connect: can not connect to bootstrap server: {:?}", bootstrap)));
            },
        };

        // TODO: move it to BrokerConnection
        debug!("Connected to {:?}", broker);
        */


        let bootstrap2 = bootstrap.clone();
        let cluster = Cluster {
            bootstrap,
            operation_timeout: operation_timeout.unwrap_or(Duration::from_secs(5)),
            brokers_maps: BrokersMaps::default(),
            resolver: ResolverHandle::new(bootstrap2),
            rx
        };

        debug!("Cluster created");

        cluster
    }

    async fn handle(&mut self, msg: Msg) {
        match msg {
            // TODO: make sure that consumer keeps its copy of leader map and does not request it every time produce message is sent
            Msg::GetLeaderMap(topics, response) => {
                let leaders = self.get_known_broker_map().await;
                let known_maps: HashSet<&String> = leaders.iter().flat_map(|i| i.1.iter().map(|t| &t.0)).collect();
                let missing_topics: Vec<_> = topics.iter().filter(|t| !known_maps.contains(t)).collect();
                for topic in missing_topics {
                    debug!("Cluster: start resolving '{}'", topic);
                    self.resolver.start_resolve(topic.clone()).await;
                }

                if response.send(leaders).is_err() {
                    error!("Failed to send leader map response");
                }
            }
        }
    }

    /// Return leader map of known brokers. Unknown brokers will be requested in background.
    async fn get_known_broker_map(&self) -> LeaderMap {
        let mut res: HashMap<BrokerId, HashMap<String,Vec<Partition>>> = HashMap::new();
        for (topic, partitions) in &self.brokers_maps.meta_cache {
            for (partition, leaderId) in partitions.iter().enumerate() {
                if let Some(leaderId) = leaderId {
                    res.entry(*leaderId).or_default()
                        .entry(topic.clone()).or_default()
                        .push(partition as u32);
                }
            }
        }
        res.into_iter().map(|t| (t.0, t.1.into_iter().collect())).collect()
    }

    #[instrument(level="debug", skip(broker_id, self))]
    pub(crate) async fn broker_get_no_connect(&self, broker_id: BrokerId) -> BrokerResult<&ConnectionHandle> {
        match self.brokers_maps.broker_id_map.get(&broker_id) {
            Some(broker) => Ok(broker),
            None => Err(BrokerFailureSource::NoBrokerAvailable)
        }
    }

    fn update_brokers_map(&mut self, meta: &protocol::MetadataResponse0) {
        debug!("Updated brokers map");
        for topic in &meta.topics {
            *(self.brokers_maps.meta_cache.entry(topic.topic.clone()).or_default()) = topic.partition_metadata.iter().map(|p|
                    if p.error_code.is_ok() { Some(p.leader) } else { None }
                ).collect();
        }


        for broker in &meta.brokers {
            match (broker.host.as_str(), broker.port as u16).to_socket_addrs() {
                Ok(addr) => {
                    let addr: Vec<SocketAddr> = addr.collect();
                    if !addr.is_empty() {
                        self.brokers_maps.broker_addr_map.insert(broker.node_id, addr[0]);
                    }
                }
                Err(e) => error!("Resolve broker error: {}", e),
            }
        }
    }

    pub(crate) async fn reset_broker(&mut self, broker_id: BrokerId) {
        self.brokers_maps.broker_id_map.remove(&broker_id);
        self.brokers_maps.broker_addr_map.remove(&broker_id);
    }

    // // TODO: what is the list of all known brokers?
    // // TODO: retry policy
    // /// Execute request on every broker until success
    // pub async fn request_any<R: protocol::Request>(&self, request: R) -> BrokerResult<R::Response> {
    //     // TODO:
    //     // Do not block broker list for the duration of the walk along all brokers.
    //     // Instead walk by the index, until index is out of size.
    //     // It is possible that some other process have modified the broker list while walk is
    //     // performed, but this is ok for "from any broker" query.
    //
    //     for broker in self.brokers_maps.broker_id_map.values() {
    //         match broker.send_request(&request).await {
    //             Ok(resp) => return Ok(resp),
    //             Err(e) => {
    //                 info!("Error {:?}", e)
    //             }
    //         }
    //     }
    //
    //     // TODO: after connect we have list of broker addresses but can not know their ID yet.
    //     // Because of that, I have to check 2 lists, brokers with known ID and without (bootstrap ones)
    //     for addr in &self.bootstrap {
    //         match ConnectionHandle::connect(*addr).await {
    //             Ok(broker) => match broker.send_request(&request).await {
    //                 Ok(resp) => return Ok(resp),
    //                 Err(e) => {
    //                     info!("Error {}", e)
    //                 }
    //             },
    //             Err(e) => info!("Error fetching topic meta: {}", e),
    //         }
    //     }
    //
    //     Err(BrokerFailureSource::NoBrokerAvailable)
    // }

    // pub async fn start_resolving_topics<T>(&self, topics: T) -> std::result::Result<(), InternalError>
    //     where T: IntoIterator, T::Item: AsRef<str>
    // {
    //     for topic in topics {
    //         self.resolver_tx.send(resolver::Cmd::ResolveTopic(topic.as_ref().to_string())).await
    //             .map_err(|e| InternalError::Critical("Failed to send topic to topic resolver".into()))?;
    //     }
    //     Ok(())
    // }

    // pub async fn start_resolving_topic(&self, topic: String) -> BrokerResult<()> {
    //     self.resolver_tx.send(resolver::Cmd::ResolveTopic(topic)).await
    //         //.map_err(|e| InternalError::Critical("Failed to send topic to topic resolver".into()))
    // }

    // /// TODO: I can not return `&protocol::MetadataResponse0` if I take a lock inside this function.
    // ///   The only way I've found so far is to return the guard which guarantees that data is inside.
    // ///   Try to find a more elegant way. Lock-free data structures?
    // async fn get_or_request_meta(&self, topic: &str) -> Result<RwLockReadGuard<'_, BrokersMaps>, BrokerFailureSource> {
    //     let maps = self.brokers_maps.read().await;
    //     if let Some(topics_meta) = maps.topics_meta.get(topic) {
    //         return Ok(maps);
    //     }
    //
    //
    //     // Did not have meta. Fetch and cache.
    //     // TODO: instead of loop use recovery policy
    //     // TODO: will wait for *all* partitions to become available. Could progress on what's up for now
    //     // and await only for failed ones?
    //     // loop {
    //     //     debug!("Fetching topic meta from server");
    //     let meta = self.fetch_topic_meta_no_update(&[topic]).await?;
    //     assert!(topic.eq_ignore_ascii_case(&meta.topics[0].topic));
    //     let topic_metadata = &meta.topics[0];
    //
    //     meta.topics[0].error_code.as_result()?;
    //
    //     if topic_metadata.partition_metadata.iter().all(|m| m.error_code == ErrorCode::None)
    //     {
    //         // No errors in partitions, just save the metadata
    //         let mut maps = self.brokers_maps.write().await;
    //         maps.topics_meta.insert(topic.to_string(), topic_metadata.into());
    //         // let meta = maps.topics_meta.get(topic).unwrap();
    //         return Ok(maps.downgrade());
    //     } else {
    //         // Some partitions have an error, return an error
    //         let errors: Vec<_> = topic_metadata.partition_metadata.iter().filter(|m| m.error_code != ErrorCode::None).map(|m| (m.partition, m.error_code)).collect();
    //         for (partition, error) in &errors
    //         {
    //             event!(target: "get_or_request_meta", tracing::Level::WARN, error_code = ?error, partition = ?partition);
    //         }
    //         // TODO: check either error is recoverable
    //         //continue;
    //         // TODO: returning just 1st error, should handle them individually somehow?
    //         return Err(BrokerFailureSource::KafkaErrorCode(errors[0].1));
    //     }
    // }

    // async fn get_or_request_meta_many(&self, topics: &[&str]) -> Result<RwLockReadGuard<'_, BrokersMaps>, BrokerFailureSource> {
    //     let maps = self.brokers_maps.read().await;
    //     let missing: Vec<&str> = topics.into_iter().filter(|t| !maps.topics_meta.contains_key(**t)).map(|t| *t).collect();
    //
    //     if !missing.is_empty() {
    //         let meta = self.fetch_topic_meta_no_update(&missing).await?;
    //         self.update_meta(&meta);
    //     }
    //
    //     todo!()
    // }

    // pub async fn get_or_request_leader_map<'a>(&self, topics: &[impl AsRef<str>]) -> BrokerResult<Vec<(BrokerId, Vec<(&'a str, Vec<Partition>)>)>> {
    //     let map = self.brokers_maps.read().await;
    //     //map.topics_meta
    //     todo!()
    // }

    // /// Used by UI, does not retry internal topic or partition errors
    // pub async fn fetch_topic_meta_owned(&self, topics: &[&str]) -> BrokerResult<protocol::MetadataResponse0> {
    //     let meta = self.fetch_topic_meta_no_update(topics).await?;
    //     Ok(meta.clone())
    // }

    // pub async fn get_topic_partition_count(&self, topic: &str) -> BrokerResult<u32> {
    //     let meta = self.brokers_maps.topics_meta.get(topic);
    //     Ok(meta.partitions.len() as u32)
    // }
    //
    // pub async fn mark_leader_down(&self, leader: BrokerId) {
    //     todo!()
    // }

    async fn update_meta(&mut self, meta: &protocol::MetadataResponse0) {
        // let mut maps = self.brokers_maps.write().await;
        for topic in &meta.topics {
            if topic.error_code != ErrorCode::None {
                continue
            }
            let topic_meta = self.brokers_maps.topics_meta.entry(topic.topic.clone()).or_insert_with(||
                TopicMeta {
                    topic: topic.topic.clone(),
                    partitions: (0..topic.partition_metadata.len()).map(|_| None).collect()
                });
            for partition in &topic.partition_metadata {
                if partition.error_code != ErrorCode::None {
                    continue
                }
                topic_meta.partitions[partition.partition as usize] = Some(partition.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use std::env;
    use log::LevelFilter;

    #[tokio::test]
    async fn resolve() {
        simple_logger::SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
        let bootstrap = vec!["127.0.0.1:9092".parse().unwrap()];

        let mut cluster = ClusterHandler::new(bootstrap, Some(Duration::from_secs(10)));
        let leaders = cluster.resolve(vec!["test1".into()]).await;
        debug!("Resolved topic: {:?}", leaders);
        tokio::time::sleep(Duration::from_secs(3)).await;
        let leaders = cluster.resolve(vec!["test1".into()]).await;
        debug!("Resolved topic: {:?}", leaders);

    }
}
