//! Cluster provides:
//! * Topic partitioning resolution
//! * Cluster topology caching
//! * Request routing

use std::borrow::BorrowMut;
use crate::error::{BrokerResult, BrokerFailureSource};
use crate::protocol;
use crate::types::*;
use crate::utils::{resolve_addr, TracedMessage};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tokio::time::Duration;
use tracing_attributes::instrument;
use tokio::sync::{mpsc, oneshot};
use crate::connection::BrokerConnection;
use std::fmt::{Debug, Formatter};
use anyhow::anyhow;
use indexmap::IndexMap;
use itertools::Itertools;
use tracing::{debug_span, Instrument, Level, span};
use crate::protocol::{MetadataResponse0, MetadataRequest0, ListOffsetsRequest0};
use log::{debug, warn, error};
use crate::metadiscover::MetaDiscover;

type LeaderMap = Vec<(BrokerId, Vec<(String, Vec<Partition>)>)>;

struct Cluster {
    bootstrap: Vec<SocketAddr>,
    operation_timeout: Duration,
    connections: IndexMap<BrokerId, BrokerConnection>,
    broker_addr_map: HashMap<BrokerId, SocketAddr>,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    leader_cache: HashMap<String, Vec<Option<BrokerId>>>,
    topics_meta: HashMap<String, TopicMeta>,
    meta_discover: MetaDiscover,
    meta_discover_tx: mpsc::Sender<String>,
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
    /// Connect to at least one broker successfully
    pub fn new(bootstrap: String, operation_timeout: Option<Duration>) -> Self {
        let bootstrap = resolve_addr(&bootstrap);
        let (meta_discover_tx, rx) = mpsc::channel(1);
        Cluster {
            bootstrap: bootstrap.clone(),
            operation_timeout: operation_timeout.unwrap_or(Duration::from_secs(5)),
            // connections: HashMap::new(),
            connections: IndexMap::new(),
            broker_addr_map: HashMap::new(),
            leader_cache: HashMap::new(),
            topics_meta: HashMap::new(),
            meta_discover: MetaDiscover::new(bootstrap, rx),
            meta_discover_tx
        }
    }

    /// Return leader map of known brokers. Unknown brokers will be requested in background.
    #[instrument(level="debug")]
    fn get_known_broker_map(&self) -> LeaderMap {
        let mut res: HashMap<BrokerId, HashMap<String,Vec<Partition>>> = HashMap::new();
        for (topic, partitions) in &self.leader_cache {
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

    #[instrument(ret)]
    fn group_leader_by_topic<'a>(&'a self, topics: &'a[String]) -> HashMap<BrokerId, Vec<String>> {
        let mut res: HashMap<BrokerId, Vec<String>> = HashMap::new();

        for topic in topics {
            match self.leader_cache.get(topic) {
                Some(brokers) => {
                    for (partition, leaderId) in brokers.into_iter().enumerate() {
                        if let Some(leaderId) = leaderId {
                            res.entry(*leaderId).or_default().push(topic.to_owned());
                        }
                    }
                },
                None => warn!("topic not found: {}", topic)
            }
        }

        res
    }

    // fn connect_if_not_connected(&mut self, broker_id: BrokerId) {
    //     let mut new_connections = vec![];
    //     if !self.connections.contains_key(&broker_id) {
    //         if let Some(addr) = self.broker_addr_map.get(&broker_id) {
    //             let conn = BrokerConnection::connect(*addr);
    //             new_connections.push((broker_id, conn));
    //         }
    //     }
    //
    //     for (brokerId, conn) in new_connections {
    //         self.connections.insert(brokerId, conn);
    //     }
    // }

    #[instrument(level="debug", skip(broker_id, self), err)]
    pub(crate) async fn broker_get_no_connect(&self, broker_id: BrokerId) -> BrokerResult<&BrokerConnection> {
        match self.connections.get(&broker_id) {
            Some(broker) => Ok(broker),
            None => Err(BrokerFailureSource::NoBrokerAvailable)
        }
    }

    fn update_brokers_map(&mut self, meta: &protocol::MetadataResponse0) {
        debug!("Updated brokers map");
        for topic in &meta.topics {
            *(self.leader_cache.entry(topic.topic.clone()).or_default()) = topic.partition_metadata.iter().map(|p|
                    if p.error_code.is_ok() { Some(p.leader) } else { None }
                ).collect();
        }


        for broker in &meta.brokers {
            match (broker.host.as_str(), broker.port as u16).to_socket_addrs() {
                Ok(addr) => {
                    let addr: Vec<SocketAddr> = addr.collect();
                    if !addr.is_empty() {
                        self.broker_addr_map.insert(broker.node_id, addr[0]);
                    }
                }
                Err(e) => error!("Resolve broker error: {}", e),
            }
        }
    }

    #[instrument]
    pub(crate) async fn reset_broker(&mut self, broker_id: BrokerId) {
        self.connections.remove(&broker_id);
        self.broker_addr_map.remove(&broker_id);
    }

    /// Get partitions count from cache or listen in background fore resolution
    #[instrument]
    async fn get_or_fetch_partition_count(&mut self, topic: String, respond_to: oneshot::Sender<BrokerResult<usize>>) {
        if let Some(meta) = self.topics_meta.get(&topic) {
            if let Err(e) = respond_to.send(Ok(meta.partitions.len())) {
                // TODO: should panic?
                error!("get_or_fetch_partition_count: failed to send response");
            }
            return
        }

        tracing::debug!("No meta in cache, fetching it");
        if let Err(e) = self.get_or_request_meta_many(&vec![topic.clone()]).await {
            let _ = respond_to.send(Err(e));
            return;
        }

        tracing::debug!("Fetched topic");
        let res = match self.topics_meta.get(&topic) {
            Some(meta) => Ok(meta.partitions.len()),
            None => Err(BrokerFailureSource::Internal(anyhow!("Failed to get meta after resolution")))
        };
        tracing::debug!("Sending response meta");
        if let Err(_) = respond_to.send(res) {
            tracing::error!("The requester dropped");
        }

        // let mut listener = self.resolver.listener();
        // // TODO: race condition, what if answer comes before listening happen?
        // self.resolver.start_resolve(vec![topic.clone()]).await;
        // // TODO: configure timeout
        // tokio::spawn(tokio::time::timeout(Duration::from_secs(3*60),async move {
        //     loop {
        //         match listener.recv().await {
        //             Ok(meta) => {
        //                  if let Some(meta) = meta.topics.iter().find(|t| t.topic == topic) {
        //                      if let Err (e) = respond_to.send(Ok(topic.len())) {
        //                          error!("get_or_fetch_partition_count: failed to send response");
        //                      }
        //                      break;
        //                  }
        //             }
        //             Err(e) => {
        //                 respond_to.send(Err(BrokerFailureSource::Internal(e.into())));
        //                 break;
        //             }
        //         }
        //     }
        // }.instrument(debug_span!("reading resolver"))));
    }

    #[instrument(ret, err)]
    async fn list_offsets(&mut self, topics: Vec<String>) -> BrokerResult<Vec<BrokerResult<protocol::ListOffsetsResponse0>>> {
        let mut res = vec![];
        self.get_or_request_meta_many(&topics).await?;
        let broker_topics = self.group_leader_by_topic(&topics);

        for broker_id in broker_topics.keys() {
            self.ensure_broker_connected(broker_id).await;
        }

        for (broker_id, topics) in broker_topics {
            if let Some(conn_idx) = self.connections.get_index_of(&broker_id) { //self.connections.get_mut(&broker_id) {
                let offset_request = ListOffsetsRequest0 {
                    replica_id: -1,
                    topics: topics.into_iter()
                        .filter_map(|topic| self.topics_meta.get(&topic)
                            .map(|meta| {
                                protocol::Topics {
                                        topic: topic.to_owned(),
                                        partitions: (0..meta.partitions.len()).map( | partition | {
                                        protocol::Partition {
                                            partition: partition as u32,
                                            // TODO: pass start/end offset
                                            //  max timestamp for message offsets. returns the largest offsets that
                                            //  are before the given time. Pass -2L for TopicHead, and -1L for TopicTail
                                            timestamp: -1,
                                            max_num_offsets: 2
                                        }
                                        }).collect_vec()
                                }
                            })
                        ).collect_vec()
                };
                // TODO: execute requests in parallel
                let (_, conn) = self.connections.get_index_mut(conn_idx).expect("Could not find connection at index");
                let response =  conn.exchange(&offset_request).await;
                res.push(response)
            } else {
                tracing::warn!("Can't find connection with brokerId: {}. Known brokers: {:?}", broker_id, self.broker_addr_map);
            }
        }

        Ok(res)
    }

    async fn ensure_broker_connected(&mut self, broker_id: &BrokerId) -> BrokerResult<()> {
        if self.connections.contains_key(broker_id) {
            return Ok(());
        }
        if let Some(addr) = self.broker_addr_map.get(broker_id) {
            let conn = BrokerConnection::connect(addr.clone()).await?;
            self.connections.insert(*broker_id, conn);
        }

        Ok(())
    }


    // // TODO: what is the list of all known brokers?
    // // TODO: retry policy
    // /// Execute request on every broker until success
    #[instrument]
    pub async fn request_any<R: protocol::Request + Debug>(&mut self, request: R) -> BrokerResult<R::Response> {
        for broker in self.connections.values_mut() {
            match broker.exchange(&request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    tracing::debug!("Error {:?}", e)
                }
            }
        }

        // TODO: after connect we have list of broker addresses but can not know their ID yet.
        //      Because of that, I have to check 2 lists, brokers with known ID and without (bootstrap ones)
        // for addr in &self.bootstrap {
        //     match ConnectionHandle::connect(*addr).await {
        //         Ok(broker) => match broker.send_request(&request).await {
        //             Ok(resp) => return Ok(resp),
        //             Err(e) => {
        //                 info!("Error {}", e)
        //             }
        //         },
        //         Err(e) => info!("Error fetching topic meta: {}", e),
        //     }
        // }

        for addr in &self.bootstrap {
            match BrokerConnection::connect(addr.clone()).await?.exchange(&request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => tracing::debug!("Error: {:?}", e)
            }
        }

        Err(BrokerFailureSource::NoBrokerAvailable)
    }

    // /// TODO: I can not return `&protocol::MetadataResponse0` if I take a lock inside this function.
    // ///   The only way I've found so far is to return the guard which guarantees that data is inside.
    // ///   Try to find a more elegant way. Lock-free data structures?
    // async fn get_or_request_meta(&self, topic: &str) -> Result<RwLockReadGuard<'_, BrokersMaps>, BrokerFailureSource> {
    //     let maps = self.read().await;
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
    //         let mut maps = self.write().await;
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

    #[instrument(ret, err)]
    async fn get_or_request_meta_many(&mut self, topics: &Vec<String>) -> BrokerResult<()> {
        // let maps = self.read().await;
        let missing = topics.into_iter()
            .filter(|topic| !self.topics_meta.contains_key(*topic))
            .map(|topic| topic.to_owned())
            .collect_vec();
        tracing::debug!("Missing topics: {:?}", missing);

        if !missing.is_empty() {
            let meta = self.fetch_topic_meta_no_update(missing).await?;
            self.update_meta(&meta).await;
        }

        Ok(())
    }

    /// Used by UI, does not retry internal topic or partition errors
    // #[instrument]
    // pub async fn fetch_topic_meta_owned(&self, topics: Vec<String>) -> BrokerResult<protocol::MetadataResponse0> {
    //     let meta = self.fetch_topic_meta_no_update(topics).await?;
    //     Ok(meta.clone())
    // }

    #[instrument(ret, err)]
    async fn fetch_topic_meta_no_update(&mut self, topics: Vec<String>) -> BrokerResult<MetadataResponse0> {
        // TODO: use resolver?
        let request = MetadataRequest0 { topics };
        self.request_any(request).await
    }


    #[instrument(res)]
    async fn update_meta(&mut self, meta: &protocol::MetadataResponse0) {
        for topic in &meta.topics {
            if !topic.error_code.is_ok() {
                continue
            }
            let topic_meta = self.topics_meta.entry(topic.topic.clone()).or_insert_with(||
                TopicMeta {
                    topic: topic.topic.clone(),
                    partitions: (0..topic.partition_metadata.len()).map(|_| None).collect()
                });
            for partition in &topic.partition_metadata {
                if !partition.error_code.is_ok() {
                    continue
                }
                topic_meta.partitions[partition.partition as usize] = Some(partition.into())
            }

            // leader_cache
            let leaders = self.leader_cache.entry(topic.topic.clone())
                .or_insert_with(|| (0..topic.partition_metadata.len()).map(|_| None ).collect());
            for partition in &topic.partition_metadata {
                if !partition.error_code.is_ok() {
                    continue
                }
                leaders[partition.partition as usize] = Some(partition.leader);
            }
        }

        for broker in &meta.brokers {
            if let Ok(addr) = broker.host.parse::<IpAddr>() {
                self.broker_addr_map.insert(broker.node_id, SocketAddr::new(addr, broker.port as u16));
                continue
            }
            if let Ok(addr) = (broker.host.as_str(), broker.port as u16).to_socket_addrs() {
                let addr: Vec<_> = addr.collect();
                if !addr.is_empty() {
                    self.broker_addr_map.insert(broker.node_id, addr[0]);
                    continue;
                }
            }

            tracing::warn!("failed to parse broker host: {}", &broker.host);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use std::env;
    use log::LevelFilter;
    use tracing::Instrument;
    use crate::utils::init_tracer;

    // #[tokio::test]
    // #[instrument(level="info")]
    // async fn resolve_topic() {
    //     let _tracer = init_tracer("test");
    //     async {
    //         simple_logger::SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
    //         let bootstrap = "127.0.0.1:9092".to_string();
    //
    //         let mut cluster = Cluster::new(bootstrap, Some(Duration::from_secs(10)));
    //         let leaders = cluster.resolve(vec!["test1".into()]).await;
    //         debug!("Resolved topic: {:?}", leaders);
    //         //tokio::time::sleep(Duration::from_secs(3)).instrument(tracing::info_span!("sleep")).await;
    //         //let leaders = cluster.resolve(vec!["test1".into()]).await;
    //         //debug!("Resolved topic: {:?}", leaders);
    //
    //         let count = cluster.get_or_fetch_partition_count("test1".into()).await.unwrap();
    //         println!("partitions: {}", count);
    //     }.instrument(tracing::info_span!("resolve-test")).await
    // }

    #[tokio::test]
    #[instrument]
    async fn fetch_offsets() {
        let _tracer = init_tracer("test");
        simple_logger::SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
        let bootstrap = "127.0.0.1:9092".to_string();

        // TODO: when brokers are down, `some_offsets: Ok([])` is returned. Return actual error.

        let mut cluster = Cluster::new(bootstrap, Some(Duration::from_secs(10)));
        let zero_offsets = cluster.list_offsets(vec!["test1".to_owned()]).await;
        let some_offsets = cluster.list_offsets(vec!["topic2".to_owned()]).await;
        println!("zero_offsets: {:?}", zero_offsets);
        println!("some_offsets: {:?}", some_offsets);
    }
}
