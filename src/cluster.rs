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
use tokio::sync::{mpsc, oneshot, RwLock};
use crate::connection::BrokerConnection;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use anyhow::anyhow;
use futures_util::AsyncWriteExt;
use indexmap::IndexMap;
use itertools::Itertools;
use tracing::{debug, warn, error, debug_span, Instrument, Level, span};
use crate::protocol::{MetadataResponse0, MetadataRequest0, ListOffsetsRequest0};
use crate::meta_cache::{Data, MetaCache};
use crate::metadiscover::MetaDiscover;

pub struct Cluster {
    bootstrap: Vec<SocketAddr>,
    operation_timeout: Duration,
    connections: IndexMap<BrokerId, BrokerConnection>,
    // broker_addr_map: HashMap<BrokerId, SocketAddr>,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    // leader_cache: HashMap<String, Vec<Option<BrokerId>>>,
    // topics_meta: HashMap<String, TopicMeta>,
    meta_cache: MetaCache,

    // meta_discover: MetaDiscover,
    meta_discover_tx: mpsc::Sender<String>,     // TODO: send `Vec<String> to ensure batching resolution?
}

impl Debug for Cluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
        .field("bootstrap", &self.bootstrap)
        .field("operation_timeout", &self.operation_timeout)
        // TODO: need to block maps to get their string
        .finish()
    }
}

impl Cluster {
    /// Connect to at least one broker successfully
    pub fn new(bootstrap: String, operation_timeout: Option<Duration>) -> Self {
        let bootstrap = resolve_addr(&bootstrap);

        let(meta_discover_tx, meta_discover_rx) = MetaDiscover::new(bootstrap.clone());
        let meta_cache = MetaCache::new();
        Self::spawn_discover(meta_cache.clone_data(), meta_discover_rx);

        Cluster {
            bootstrap,
            operation_timeout: operation_timeout.unwrap_or(Duration::from_secs(5)),
            connections: IndexMap::new(),
            meta_cache,
            meta_discover_tx
        }
    }

    fn spawn_discover(data: Arc<RwLock<Data>>, mut meta_discover_rx: mpsc::Receiver<BrokerResult<protocol::MetadataResponse0>>) {
        tokio::task::spawn(async move {
            loop {
                debug!("spawn_discover: awaiting for meta update");
                match meta_discover_rx.recv().await {
                    Some(meta) => {
                        debug!("Got meta update");
                        let mut data = (*data).write().await;
                        match meta {
                            Ok(meta) => {
                                data.update_meta(&meta);
                                
                            },
                            Err(e) => tracing::debug!("Received error from discovery: {e}")
                        }
                    },
                    None => break
                }
            }
        });
    }

    // /// Return leader map of known brokers. Unknown brokers will be requested in background.
    // #[instrument(level="debug")]
    // pub(crate) fn get_known_broker_map(&self) -> LeaderMap {
    //     let mut res: HashMap<BrokerId, HashMap<String,Vec<Partition>>> = HashMap::new();
    //     
    //     for (topic, partitions) in &self.leader_cache {
    //         for (partition, leaderId) in partitions.iter().enumerate() {
    //             if let Some(leaderId) = leaderId {
    //                 res.entry(*leaderId).or_default()
    //                     .entry(topic.clone()).or_default()
    //                     .push(partition as u32);
    //             }
    //         }
    //     }
    //     res.into_iter().map(|t| (t.0, t.1.into_iter().collect())).collect()
    // }

    // #[instrument(ret)]
    // fn group_leader_by_topic<'a>(&'a self, topics: &'a[String]) -> HashMap<BrokerId, Vec<String>> {
    //     let mut res: HashMap<BrokerId, Vec<String>> = HashMap::new();
    //
    //     for topic in topics {
    //         match self.leader_cache.get(topic) {
    //             Some(brokers) => {
    //                 for (partition, leaderId) in brokers.into_iter().enumerate() {
    //                     if let Some(leaderId) = leaderId {
    //                         res.entry(*leaderId).or_default().push(topic.to_owned());
    //                     }
    //                 }
    //             },
    //             None => warn!("topic not found: {}", topic)
    //         }
    //     }
    //
    //     res
    // }

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

    // fn update_brokers_map(&mut self, meta: &protocol::MetadataResponse0) {
    //     debug!("Updated brokers map");
    //     for topic in &meta.topics {
    //         *(self.leader_cache.entry(topic.topic.clone()).or_default()) = topic.partition_metadata.iter().map(|p|
    //                 if p.error_code.is_ok() { Some(p.leader) } else { None }
    //             ).collect();
    //     }
    //
    //
    //     for broker in &meta.brokers {
    //         match (broker.host.as_str(), broker.port as u16).to_socket_addrs() {
    //             Ok(addr) => {
    //                 let addr: Vec<SocketAddr> = addr.collect();
    //                 if !addr.is_empty() {
    //                     self.broker_addr_map.insert(broker.node_id, addr[0]);
    //                 }
    //             }
    //             Err(e) => error!("Resolve broker error: {}", e),
    //         }
    //     }
    // }

    // #[instrument]
    // pub(crate) async fn reset_broker(&mut self, broker_id: BrokerId) {
    //     self.connections.remove(&broker_id);
    //     self.broker_addr_map.remove(&broker_id);
    // }

    /// Get partitions count from cache or listen in background fore resolution
    #[instrument]
    pub(crate) async fn get_or_fetch_partition_count(&mut self, topic: String) -> BrokerResult<usize> {
        let meta = self.meta_cache.get_or_await(move |cache| cache.topics_meta.get(&topic).cloned()).await;
        // if let Some(meta) = meta {
        //     return Ok(meta.partitions.len());
        // }

        match meta {
            Some(meta) => Ok(meta.partitions.len()),
            None => Err(BrokerFailureSource::Internal(anyhow!("Failed to fetch count")))
        }

        // tracing::debug!("No meta in cache, fetching it");
        // if let Err(e) = self.get_or_request_meta_many(&vec![topic.clone()]).await {
        //     // let _ = respond_to.send(Err(e));
        //     return Err(e);
        // }
        //
        // tracing::debug!("Fetched topic");
        // match self.topics_meta.get(&topic) {
        //     Some(meta) => Ok(meta.partitions.len()),
        //     None => Err(BrokerFailureSource::Internal(anyhow!("Failed to get meta after resolution")))
        // }
    }

    #[instrument(ret, err)]
    pub async fn list_offsets(&mut self, topics: Vec<String>) -> BrokerResult<Vec<BrokerResult<protocol::ListOffsetsResponse0>>> {
        let mut res = vec![];
        let (broker_topics, topics_meta) = self.get_or_request_brokers_and_meta(&topics).await?;
        //let broker_topics = self.meta_cache.group_leader_by_topic(&topics).await;

        for broker_id in broker_topics.keys() {
            self.ensure_broker_connected(broker_id).await?;
        }

        //let topics_meta = self.meta_cache.get_topics_meta(&topics).await;

        for (broker_id, topics) in broker_topics {
            if let Some(conn_idx) = self.connections.get_index_of(&broker_id) { //self.connections.get_mut(&broker_id) {
                let offset_request = ListOffsetsRequest0 {
                    replica_id: -1,
                    topics: topics.into_iter()
                        .filter_map(|topic| topics_meta.get(&topic)
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
                tracing::warn!("Can't find connection with brokerId: {}", broker_id);
            }
        }

        Ok(res)
    }

    // fn start_discover(&self, )

    async fn ensure_broker_connected(&mut self, broker_id: &BrokerId) -> BrokerResult<()> {
        if self.connections.contains_key(broker_id) {
            return Ok(());
        }

        // TODO: if broker does not exist, return error?
        if let Some(addr) = self.meta_cache.get_addr_by_broker(broker_id).await {
            let conn = BrokerConnection::connect(addr).await?;
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
            match BrokerConnection::connect(*addr).await?.exchange(&request).await {
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
    async fn get_or_request_brokers_and_meta(&mut self, topics: &Vec<String>) -> BrokerResult<(HashMap<BrokerId, Vec<String>>, HashMap<String, TopicMeta>)> {
        let mut topics_meta = self.meta_cache.get_topics_meta(topics).await;

        let missings: Vec<_> = topics.into_iter()
            //.partition(self.topics_meta.contains_key(*topic));
            .filter(|t| !topics_meta.contains_key(*t)).collect();
        tracing::debug!("Missing topics, will request resolution: {:?}", missings);

        for missing in &missings {
            self.meta_discover_tx.send((*missing).clone()).await
                .map_err(|e| BrokerFailureSource::Internal(e.into()))?
        }

        tracing::debug!("Awaiting for missing topics to resolve");
        for missing in missings {
            let meta = self.meta_cache.get_or_await(|data| data.topics_meta.get(missing).cloned()).await;
            match meta {
                Some(meta) => {topics_meta.insert(meta.topic.clone(), meta.clone()); },
                // TODO: get_or_await should return `Err`
                None => return Err(BrokerFailureSource::Internal(anyhow!("Can't resolve topic: {}", missing)))
            }
        }

        let brokers_topic = self.meta_cache.group_leader_by_topics(topics).await;

        Ok((brokers_topic, topics_meta))
    }

    /// Used by UI, does not retry internal topic or partition errors
    // #[instrument]
    // pub async fn fetch_topic_meta_owned(&self, topics: Vec<String>) -> BrokerResult<protocol::MetadataResponse0> {
    //     let meta = self.fetch_topic_meta_no_update(topics).await?;
    //     Ok(meta.clone())
    // }

    #[instrument(ret, err)]
    pub async fn fetch_topic_meta_no_update(&mut self, topics: Vec<String>) -> BrokerResult<MetadataResponse0> {
        // TODO: use resolver?
        let request = MetadataRequest0 { topics };
        self.request_any(request).await
    }


    // #[instrument(res)]
    // async fn update_meta(&mut self, meta: &protocol::MetadataResponse0) {
    //     for topic in &meta.topics {
    //         if !topic.error_code.is_ok() {
    //             continue
    //         }
    //         let topic_meta = self.topics_meta.entry(topic.topic.clone()).or_insert_with(||
    //             TopicMeta {
    //                 topic: topic.topic.clone(),
    //                 partitions: (0..topic.partition_metadata.len()).map(|_| None).collect()
    //             });
    //         for partition in &topic.partition_metadata {
    //             if !partition.error_code.is_ok() {
    //                 continue
    //             }
    //             topic_meta.partitions[partition.partition as usize] = Some(partition.into())
    //         }
    //
    //         // leader_cache
    //         let leaders = self.leader_cache.entry(topic.topic.clone())
    //             .or_insert_with(|| (0..topic.partition_metadata.len()).map(|_| None ).collect());
    //         for partition in &topic.partition_metadata {
    //             if !partition.error_code.is_ok() {
    //                 continue
    //             }
    //             leaders[partition.partition as usize] = Some(partition.leader);
    //         }
    //     }
    //
    //     for broker in &meta.brokers {
    //         if let Ok(addr) = broker.host.parse::<IpAddr>() {
    //             self.broker_addr_map.insert(broker.node_id, SocketAddr::new(addr, broker.port as u16));
    //             continue
    //         }
    //         if let Ok(addr) = (broker.host.as_str(), broker.port as u16).to_socket_addrs() {
    //             let addr: Vec<_> = addr.collect();
    //             if !addr.is_empty() {
    //                 self.broker_addr_map.insert(broker.node_id, addr[0]);
    //                 continue;
    //             }
    //         }
    //
    //         tracing::warn!("failed to parse broker host: {}", &broker.host);
    //     }
    // }
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
    async fn fetch_offsets() -> anyhow::Result<()> {
        let _tracer = init_tracer("test");
        simple_logger::SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
        let bootstrap = "127.0.0.1:9092".to_string();

        // TODO: when brokers are down, `some_offsets: Ok([])` is returned. Return actual error.

        let mut cluster = Cluster::new(bootstrap, Some(Duration::from_secs(10)));
        let zero_offsets = cluster.list_offsets(vec!["test1".to_owned()]).await;
        let some_offsets = cluster.list_offsets(vec!["topic2".to_owned()]).await;
        println!("zero_offsets: {:?}", zero_offsets);
        println!("some_offsets: {:?}", some_offsets);

        let count = cluster.get_or_fetch_partition_count("test1".to_string()).await?;
        println!("count: {count}");

        Ok(())
    }
}
