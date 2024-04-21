use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::broadcast::Receiver;
use crate::protocol;
use crate::types::{BrokerId, Partition, TopicMeta};
use tracing::{debug, instrument};
use crate::cluster::LeaderMap;

#[derive(Clone)]
pub struct MetaCache {
    data: Arc<RwLock<Data>>,    // TODO: replace with std::sync::RWLock to avoid `await`s?
}

pub struct Data {
    pub(crate) broker_addr_map: HashMap<BrokerId, SocketAddr>,
    /// topic -> partition[] -> leader (if known). Leader is None if it is down and requires re-discovery
    pub(crate) leader_cache: HashMap<String, Vec<Option<BrokerId>>>,
    pub(crate) topics_meta: HashMap<String, TopicMeta>,
    pub(crate) updates: tokio::sync::broadcast::Sender<()>
}

impl MetaCache {
    pub fn new() -> Self {
        let (waiters, _) = tokio::sync::broadcast::channel(6);
        MetaCache {
            data: Arc::new(RwLock::new(Data {
                broker_addr_map: Default::default(),
                leader_cache: Default::default(),
                topics_meta: Default::default(),
                updates: waiters,
            })),
        }
    }

    pub fn subscribe_to_updates(&self) -> Receiver<()> {
        let data = self.data.read().expect("Lock failed");
        data.updates.subscribe()
    }

    pub(crate) fn clone_data(&self) -> Arc<RwLock<Data>> {
        self.data.clone()
    }

    /// Return leader map of known brokers. Unknown brokers will be requested in background.
    #[instrument(level="debug")]
    pub(crate) fn get_known_broker_map(&self) -> LeaderMap {
        let mut res: HashMap<BrokerId, HashMap<String,Vec<Partition>>> = HashMap::new();
        let data = self.data.read().expect("Lock failed");

        for (topic, partitions) in &data.leader_cache {
            for (partition, leader_id) in partitions.iter().enumerate() {
                if let Some(leader_id) = leader_id {
                    res.entry(*leader_id).or_default()
                        .entry(topic.clone()).or_default()
                        .push(partition as u32);
                }
            }
        }
        res.into_iter().map(|t| (t.0, t.1.into_iter().collect())).collect()
    }

    #[instrument(ret)]
    pub(crate) fn group_leader_by_topics<'a>(&'a self, topics: &'a[String]) -> HashMap<BrokerId, Vec<String>> {
        let mut res: HashMap<BrokerId, Vec<String>> = HashMap::new();
        let data = self.data.read().expect("Lock failed");

        for topic in topics {
            match data.leader_cache.get(topic) {
                Some(brokers) => {
                    for (partition, leaderId) in brokers.into_iter().enumerate() {
                        if let Some(leaderId) = leaderId {
                            res.entry(*leaderId).or_default().push(topic.to_owned());
                        }
                    }
                },
                None => tracing::warn!("topic not found: {}", topic)
            }
        }

        res
    }

    pub(crate) fn get_topics_meta(&self, topics: &[String]) -> HashMap<String, TopicMeta> {
        let data = self.data.read().expect("Lock failed");
        topics.iter()
            .filter_map(|t| data.topics_meta.get(t).map(|m| (t.clone(), m.clone())))
            .collect()
    }

    pub(crate) fn get_topic_meta(&self, topic: &str) -> Option<TopicMeta> {
        let data = self.data.read().expect("Lock failed");
        data.topics_meta.get(topic).cloned()
    }

    // TODO: should return BrokerResult and not Option?
    pub(crate) async fn get_or_await<T, F>(&self, getter: F) -> Option<T>
        where
            F: Fn(&Data) -> Option<T>,
    {
        let mut subscription = {
            let data = self.data.read().expect("Lock failed");
            match getter(&data) {
                Some(v) => {
                    debug!("Get value in cache");
                    return Some(v);
                }
                None => {
                    debug!("Cache miss, awaiting for result");
                    data.updates.subscribe()
                }
            }
        };

        loop {
            debug!("Awaiting for metadata change");
            if let Err(_) = subscription.recv().await {
                debug!("Meta subscription closed");
                return None;
            }
            debug!("Got change in metadata");
            let data = self.data.read().expect("Lock failed");
            match getter(&data) {
                Some(v) => {
                    debug!("Got value in cache after awaiting");
                    return Some(v);
                },
                None => debug!("Cache changed, but still no value, will await again"),
            }
        }
    }

    pub fn get_addr_by_broker(&self, broker: &BrokerId) -> Option<SocketAddr> {
        let data = self.data.read().expect("Lock failed");
        data.broker_addr_map.get(broker).cloned()
    }
}

impl Debug for MetaCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetaCache")
    }
}

impl Data {
    pub fn update_meta(&mut self, meta: &protocol::MetadataResponse0) {
        debug!("Updating meta");
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

        debug!("Updated meta cache: {self:#?}");

        self.trigger_wakers();
    }

    fn trigger_wakers(&mut self) {
        // It's ok to error when nobody is listening
        debug!("Triggering cache update event");
        let res = self.updates.send(());
        debug!("Result of waiters broadcast: {:?}", res);
    }
}

impl Debug for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaCache::Data")
            .field("broker_addr_map", &self.broker_addr_map)
            .field("leader_cache", &self.leader_cache)
            .field("topic_meta", &self.topics_meta)
            .finish()
    }
}
