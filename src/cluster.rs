//! Cluster provides:
//! * Topic partitioning resolution
//! * Cluster topology caching
//! * Request routing

use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tokio::time::Duration;
use tracing_attributes::instrument;
use tokio::sync::{mpsc, oneshot};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::IndexMut;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::{Receiver, Sender, SendError};
use anyhow::anyhow;
use futures_util::AsyncWriteExt;
use indexmap::IndexMap;
use itertools::Itertools;
use tracing::{debug, warn, error, debug_span, Instrument, Level, span};
use crate::error::{BrokerResult, BrokerFailureSource};
use crate::protocol;
use crate::types::*;
use crate::utils::{resolve_addr, TracedMessage};
use crate::connection::BrokerConnection;
use crate::protocol::{MetadataResponse0, MetadataRequest0, ListOffsetsRequest0};
use crate::meta_cache::{Data, MetaCache};
use crate::metadiscover::MetaDiscover;

pub(crate) type LeaderMap = Vec<(BrokerId, Vec<(String, Vec<Partition>)>)>;

pub struct Cluster {
    bootstrap: Vec<SocketAddr>,
    operation_timeout: Duration,
    connections: IndexMap<BrokerId, BrokerConnection>,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    meta_cache: MetaCache,
    meta_discover_tx: mpsc::Sender<String>,     // TODO: send `Vec<String> to ensure batching resolution?
}

pub struct ClusterActor {
    cluster: Cluster,
}

pub enum Cmd {

}

pub enum Response {

}

impl Debug for Cluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
        .field("bootstrap", &self.bootstrap)
        .field("operation_timeout", &self.operation_timeout)
        .field("meta_cache", &self.meta_cache)
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

    pub fn spawn_cluster(self) -> (mpsc::Sender<Cmd>, mpsc::Receiver<Response>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(2);
        let (response_tx, response_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            while let Some(msg) = cmd_rx.recv().await {
                todo!()
                // match msg {
                //
                // }
            }
        });

        (cmd_tx, response_rx)
    }

    pub fn get_meta_cache(&self) -> MetaCache {
        self.meta_cache.clone()
    }

    pub async fn request_meta_refresh(&self, topic: String) -> Result<(), mpsc::error::SendError<String>> {
        self.meta_discover_tx.send(topic).await
    }

    pub fn meta_discover_sender_clone(&self) -> mpsc::Sender<String> {
        self.meta_discover_tx.clone()
    }

    fn spawn_discover(data: Arc<RwLock<Data>>, mut meta_discover_rx: mpsc::Receiver<BrokerResult<protocol::MetadataResponse0>>) {
        tokio::task::spawn(async move {
            loop {
                debug!("spawn_discover: awaiting for meta update");
                match meta_discover_rx.recv().await {
                    Some(meta) => {
                        debug!("Got meta update");
                        let mut data = (*data).write().expect("Lock failed");
                        match meta {
                            Ok(meta) => {
                                data.update_meta(&meta);
                                
                            },
                            Err(e) => debug!("Received error from discovery: {e}")
                        }
                    },
                    None => break
                }
            }
        });
    }

    /// Return leader map of known brokers. Unknown brokers will be requested in background.
    #[instrument(level="debug")]
    pub(crate) fn get_known_broker_map(&self) -> LeaderMap {
        self.meta_cache.get_known_broker_map()
    }

    /// Get partitions count from cache or listen in background fore resolution
    #[instrument]
    pub(crate) async fn get_or_fetch_partition_count(&mut self, topic: String) -> BrokerResult<usize> {
        let _ = self.get_or_request_brokers_and_meta(&vec![topic.clone()]).await?;
        let meta = self.meta_cache.get_or_await(move |cache| cache.topics_meta.get(&topic).cloned()).await;

        match meta {
            Some(meta) => Ok(meta.partitions.len()),
            None => Err(BrokerFailureSource::Internal(anyhow!("Failed to fetch count")))
        }
    }

    #[instrument(ret, err)]
    pub async fn list_offsets(&mut self, topics: Vec<String>) -> BrokerResult<Vec<BrokerResult<protocol::ListOffsetsResponse0>>> {
        let mut res = vec![];
        let (broker_topics, topics_meta) = self.get_or_request_brokers_and_meta(&topics).await?;

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

    pub(crate) async fn broker_get_or_connect(&mut self, broker_id: BrokerId) -> BrokerResult<&mut BrokerConnection> {
        let idx = match self.connections.get_index_of(&broker_id) {
            Some(idx) => idx,
            None => {
                match self.meta_cache.get_addr_by_broker(&broker_id) {
                    Some(addr) => {
                        match BrokerConnection::connect(addr).await {
                            Ok(broker) => {
                                self.connections.insert(broker_id, broker);
                                self.connections.get_index_of(&broker_id).unwrap()
                            }
                            Err(e) => return Err(e)
                        }
                    }
                    None => return Err(BrokerFailureSource::Internal(anyhow::format_err!("Address for broker_id {} not found in metadata", broker_id)))
                }
           }
        };

        Ok(self.connections.index_mut(idx))
    }

    async fn ensure_broker_connected(&mut self, broker_id: &BrokerId) -> BrokerResult<()> {
        if self.connections.contains_key(broker_id) {
            return Ok(());
        }

        // TODO: if broker does not exist, return error?
        if let Some(addr) = self.meta_cache.get_addr_by_broker(broker_id) {
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
                    debug!("Error {:?}", e)
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
                Err(e) => debug!("Error: {:?}", e)
            }
        }

        Err(BrokerFailureSource::NoBrokerAvailable)
    }

    #[instrument(ret, err)]
    async fn get_or_request_brokers_and_meta(&mut self, topics: &Vec<String>) -> BrokerResult<(HashMap<BrokerId, Vec<String>>, HashMap<String, TopicMeta>)> {
        // TODO: extract resolving missing topics into a separate function
        let mut topics_meta = self.meta_cache.get_topics_meta(topics);

        let missings: Vec<_> = topics.into_iter()
            //.partition(self.topics_meta.contains_key(*topic));
            .filter(|t| !topics_meta.contains_key(*t)).collect();
        debug!("Missing topics, will request resolution: {:?}", missings);

        for missing in &missings {
            self.meta_discover_tx.send((*missing).clone()).await
                .map_err(|e| BrokerFailureSource::Internal(e.into()))?
        }

        debug!("Awaiting for missing topics to resolve");
        for missing in missings {
            let meta = self.meta_cache.get_or_await(move |data| data.topics_meta.get(missing).cloned()).await;
            match meta {
                Some(meta) => {topics_meta.insert(meta.topic.clone(), meta); },
                // TODO: get_or_await should return `Err`
                None => return Err(BrokerFailureSource::Internal(anyhow!("Can't resolve topic: {}", missing)))
            }
        }

        let brokers_topic = self.meta_cache.group_leader_by_topics(topics);

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
