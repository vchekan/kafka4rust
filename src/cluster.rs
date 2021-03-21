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

use crate::broker::Broker;
use crate::error::{Result, InternalError, BrokerFailureSource};
use crate::futures::{repeat_with_timeout, RepeatResult};
use crate::protocol;
use crate::types::*;
use crate::utils::resolve_addr;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::time::Duration;
use tracing_attributes::instrument;
use crate::resolver::{self, start_resolver};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};

#[derive(Debug)]
pub struct Cluster {
    bootstrap: Vec<SocketAddr>,
    broker_id_map: HashMap<BrokerId, Broker>,
    broker_addr_map: HashMap<BrokerId, SocketAddr>,
    operation_timeout: Duration,
    resolver_tx: Sender<crate::resolver::Cmd>,
    resolver_rx: Receiver<Vec<String>>,
}

impl Cluster {
    #[instrument(err)]
    pub fn with_bootstrap(bootstrap: &str, timeout: Option<Duration>) -> Result<Self> {
        let bootstrap = resolve_addr(bootstrap);
        Ok(Self::new(bootstrap, timeout))
    }

    /// Connect to at least one broker successfully .
    pub fn new(bootstrap: Vec<SocketAddr>, operation_timeout: Option<Duration>) -> Self {
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


        let (req_tx, req_rx) = mpsc::channel(1);
        let (resp_tx, resp_rx) = mpsc::channel(1);
        let cluster = Cluster {
            bootstrap,
            broker_id_map: HashMap::new(),
            broker_addr_map: HashMap::new(),
            operation_timeout: operation_timeout.unwrap_or(Duration::from_secs(5)),
            resolver_tx: req_tx,
            resolver_rx: resp_rx,
        };
        start_resolver(req_rx, resp_tx);

        cluster
    }

    /// Connect to known or seed broker and get topic metadata.
    /// If `topics` is empty, then fetch metadata for all topics.
    /// Update internal broker maps according to the response.
    #[instrument(skip(self), err)]
    pub async fn fetch_topic_meta_and_update(
        &mut self,
        topics: &[&str],
    ) -> Result<protocol::MetadataResponse0> {

        /*debug!("fetch_topic_meta_and_retry({:?})", topics);
        // TODO: wait with timeout
        for broker in self.broker_id_map.values() {
            debug!("Trying to fetch meta from known broker {:?}", broker);
            match fetch_topic_with_broker(broker, topics, self.operation_timeout).await {
                Ok(meta) => {
                    self.update_brokers_map(&meta);
                    return Ok(meta);
                }
                Err(e) => {
                    tracing::event!(tracing::Level::ERROR, "Error fetching topic meta: {}", e);
                    info!("Error fetching topic meta1: {}", e)
                }
            }
        }

        let operation_timeout = self.operation_timeout;
        let meta = repeat_with_timeout(|| async {
            for addr in &self.bootstrap {
                debug!("Trying to fetch meta from bootstrap broker {:?}", addr);
                let broker = Broker::connect(*addr).await;
                match broker {
                    Ok(broker) => {
                        let meta = fetch_topic_with_broker(&broker, topics, operation_timeout).await;
                        match meta {
                            Ok(meta) => {
                                return Ok(meta);
                            }
                            Err(e) => {
                                debug!("Failed to fetch meta from bootstrap broker {:?}. {:#?}", broker, e);
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to connect to bootstrap broker {:?}. {:#?}", addr, e);
                    }
                }
            }
            Err(anyhow!("Could not connect to any broker in bootstrap"))
        },  Duration::from_secs(1), operation_timeout).await?;
         */

        let meta = self.fetch_topic_meta_no_update(topics).await?;
        self.update_brokers_map(&meta);
        Ok(meta)
    }

    #[instrument(skip(self), err)]
    pub async fn fetch_topic_meta_no_update(
        &self,
        topics: &[&str],
    ) -> Result<protocol::MetadataResponse0> {

        // fetch from known brokers
        for broker in self.broker_id_map.values() {
            debug!("Trying to fetch meta from known broker {:?}", broker);
            match fetch_topic_with_broker(broker, topics, self.operation_timeout).await {
                Ok(meta) => return Ok(meta),
                Err(e) => {
                    tracing::event!(tracing::Level::ERROR, "Error fetching topic meta: {}", e);
                    info!("Error fetching topic meta1: {}", e)
                }
            }
        }

        // if failed to fetch from known brokers, give it a try for bootstrap ones
        let operation_timeout = self.operation_timeout;
        let meta = repeat_with_timeout(|| async {
            for addr in &self.bootstrap {
                debug!("Trying to fetch meta from bootstrap broker {:?}", addr);
                let broker = Broker::connect(*addr).await;
                match broker {
                    Ok(broker) => {
                        let meta = fetch_topic_with_broker(&broker, topics, operation_timeout).await;
                        match meta {
                            Ok(meta) => return Ok(meta),
                            Err(e) => debug!("Failed to fetch meta from bootstrap broker {:?}. {:#?}", broker, e),
                        }
                    }
                    Err(e) => debug!("Failed to connect to bootstrap broker {:?}. {:#?}", addr, e),
                }
            }
            Err(InternalError::BrokerFailure(BrokerFailureSource::NoBrokerAvailable))
        },  Duration::from_secs(1), operation_timeout).await;

        match meta {
            RepeatResult::Timeout => Err(InternalError::BrokerFailure(BrokerFailureSource::Timeout)),
            RepeatResult::Ok(res) => res
        }
    }

    #[instrument(level="debug", skip(broker_id, self))]
    pub(crate) async fn broker_get_or_connect(&mut self, broker_id: BrokerId) -> Result<&Broker> {
        let x: Result<&Broker,_> = match self.broker_id_map.entry(broker_id) {
            Occupied(entry) => Ok(entry.into_mut()),
            Vacant(entry) => {
                if let Some(addr) = self.broker_addr_map.get(&broker_id) {
                    let broker = Broker::connect(*addr).await?;
                    debug!(
                        "broker_get_or_connect: broker_id={}, connected to {}",
                        broker_id, addr
                    );
                    Ok(entry.insert(broker))
                } else {
                    Err(InternalError::BrokerFailure(BrokerFailureSource::UnknownBrokerId(broker_id)))
                }
            }
        };
        x
    }

    #[instrument(level="debug", skip(broker_id, self))]
    pub(crate) async fn broker_get_no_connect(&self, broker_id: BrokerId) -> Result<&Broker> {
        unimplemented!()
    }

    fn update_brokers_map(&mut self, meta: &protocol::MetadataResponse0) {
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

    pub(crate) fn reset_broker(&mut self, broker_id: BrokerId) {
        self.broker_id_map.remove(&broker_id);
        self.broker_addr_map.remove(&broker_id);
    }

    // TODO: what is the list of all known brokers?
    // TODO: retry policy
    /// Execute request on every broker until success
    pub async fn request<R: protocol::Request>(&self, request: R) -> Result<R::Response> {
        for broker in self.broker_id_map.values() {
            match broker.send_request(&request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    info!("Error {:?}", e)
                }
            }
        }

        // TODO: after connect we have list of broker addresses but can not know their ID yet.
        // Because of that, I have to check 2 lists, brokers with known ID and without (bootstrap ones)
        for addr in &self.bootstrap {
            match Broker::connect(*addr).await {
                Ok(broker) => match broker.send_request(&request).await {
                    Ok(resp) => return Ok(resp),
                    Err(e) => {
                        info!("Error {}", e)
                    }
                },
                Err(e) => info!("Error fetching topic meta: {}", e),
            }
        }

        Err(InternalError::BrokerFailure(BrokerFailureSource::NoBrokerAvailable))
    }

    pub async fn start_resolving_topics(&self, topics: impl Iterator<Item=&String>) -> Result<()> {
        for topic in topics {
            self.resolver_tx.send(resolver::Cmd::ResolveTopic(topic.clone())).await.map_err(|e| InternalError::Critical("Failed to send topic to topic resolver".into()))?;
        }
        Ok(())
    }
}

pub struct ClusterApi {
}

#[instrument(level="debug")]
async fn fetch_topic_with_broker(
    broker: &Broker,
    topics: &[&str],
    timeout: Duration,
) -> Result<protocol::MetadataResponse0> {
    let req = protocol::MetadataRequest0 {
        topics: topics.iter().map(|t| t.to_string()).collect(),
    };
    match tokio::time::timeout(timeout, broker.send_request(&req)).await {
        Err(_) => Err(InternalError::BrokerFailure(BrokerFailureSource::Timeout)),
        Ok(res) => res,
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use std::env;

    #[test]
    fn resolve() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();

        task::block_on(async {
            //let addr = vec!["127.0.0.1:9092".to_string()];
            let bootstrap = vec![
                //"no.such.host.com:9092".parse(),
                //env::var("kafka-bootstrap").parse().unwrap_or("127.0.0.1:9092".parse()),
                "broker1:9092".parse().unwrap()
            ];

            //let (tx, rx) = mpsc::unbounded();
            let mut cluster = Cluster::connect_with_bootstrap(bootstrap).unwrap();
            let topic_meta = cluster.resolve_topic("test1").await.unwrap();
            debug!("Resolved topic: {:?}", topic_meta);

            //info!("Bootstrapped: {:?}", cluster);
            //cluster.tx.unbounded_send(EventIn::ResolveTopic("test1".to_string())).unwrap();
            //let res = dbg!(cluster.rx.next().await);
            //debug!("Got response");
        });
    }
}
*/
