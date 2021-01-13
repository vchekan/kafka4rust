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

use crate::types::*;
use crate::broker::Broker;
use crate::error::KafkaError;
use crate::protocol;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use crate::utils::resolve_addr;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use crate::futures::repeat;
use tracing_attributes::instrument;
use anyhow::Result;
use tokio::time::Duration;

#[derive(Debug)]
pub struct Cluster {
    bootstrap: Vec<SocketAddr>,
    broker_id_map: HashMap<BrokerId, Broker>,
    broker_addr_map: HashMap<BrokerId, SocketAddr>,
}

impl Cluster {
    #[instrument]
    pub fn with_bootstrap(bootstrap: &str) -> Result<Self> {
        let bootstrap = resolve_addr(bootstrap);
        Ok(Self::new(bootstrap))
    }

    /// Connect to at least one broker successfully .
    pub fn new(bootstrap: Vec<SocketAddr>) -> Self {
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

        Cluster {
            bootstrap,
            //seed_broker: broker,
            broker_id_map: HashMap::new(),
            broker_addr_map: HashMap::new(),
        }
    }

    /// Connect to known or seed broker and get topic metadata.
    /// If `topics` is empty, then fetch metadata for all topics.
    #[instrument(skip(self))]
    pub async fn fetch_topic_meta(&mut self, topics: &[&str]) -> Result<protocol::MetadataResponse0, KafkaError> {
        debug!("fetch_topic_meta({:?})", topics);
        // TODO: wait with timeout
        for broker in self.broker_id_map.values() {
            match repeat(|| {fetch_topic_with_broker_and_retry(broker, topics)}, Duration::from_secs(1), 5).await {
                Ok(meta) => {
                    self.update_brokers_map(&meta);
                    return Ok(meta)
                },
                Err(e) => {
                    tracing::event!(tracing::Level::ERROR, "Error fetching topic meta: {}", e);
                    info!("Error fetching topic meta1: {}", e)
                },
            }
        }

        for addr in &self.bootstrap {
            match repeat(|| Broker::connect(*addr), Duration::from_secs(1), 10).await {
                Ok(broker) => match repeat(|| fetch_topic_with_broker_and_retry(&broker, topics), Duration::from_secs(1), 5).await {
                    Ok(meta) => {
                        self.update_brokers_map(&meta);
                        return Ok(meta)
                    },
                    Err(e) => info!("Error fetching topic meta2: {:#}", e),
                }
                Err(e) => info!("Error fetching topic meta3: {:#}", e),
            }
        }

        Err(KafkaError::NoBrokerAvailable("Failed to find broker to fetch topics metadata".to_owned()))

        /*

        // TODO: if failed, try other connected brokers
        let meta_response = fetch_topic_with_broker_and_retry(&self.seed_broker, &vec![topic]).await?;

        // TODO: connect in parallel
        // update known brokers
        for broker in &meta_response.brokers {
            // Looks like std::net do not support IpAddr resolution, resolve with 0 port and set port later
            let addr = (broker.host.as_str(), 0).to_socket_addrs().context(format!("Cluster: resolve host: '{}'", broker.host))?.collect::<Vec<_>>();
            if addr.len() == 0 {
                return Err(Error::DnsFailed(format!("{}:{}", broker.host, broker.port)))
            }
            let mut addr = addr[0];
            addr.set_port(broker.port as u16);
            let connected_broker = Broker::connect(addr).await.context("Cluster: resolve topic")?;
            self.broker_id_map.insert(broker.node_id, connected_broker);

            return Ok(meta_response);
        }

        // TODO: start recovery?
        Err(Error::NoBrokerAvailable)
        */

        /*
        // TODO: how to implement recovery policy?
        self.connections.iter_mut().map(|conn| {
            let req = protocol::MetadataRequest0 {topics: vec![topic]};
            let resp = conn.request(req).await?;
        })
        */
    }

    pub(crate) async fn broker_get_or_connect(&mut self, broker_id: BrokerId) -> Result<&Broker> {
        match self.broker_id_map.entry(broker_id) {
            Occupied(entry) => {
                Ok(entry.into_mut())
            }
            Vacant(entry) => {
                if let Some(addr) = self.broker_addr_map.get(&broker_id) {
                    let broker = Broker::connect(*addr).await?;
                    debug!("broker_get_or_connect: broker_id={}, connected to {}", broker_id, addr);
                    Ok(entry.insert(broker))
                } else {
                    Err(KafkaError::NoBrokerAvailable(format!("Failed to find broker for broker_id: {}", broker_id)).into())
                }
            }
        }
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


    /// Execute request on every broker until success
    /// TODO: what is the list of all known brokers?
    /// TODO: retry policy
    pub async fn request<R: protocol::Request>(&self, request: R) -> Result<R::Response> {
        for broker in self.broker_id_map.values() {
            match broker.send_request(&request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {info!("Error {}", e)},
            }
        }

        // TODO: after connect we have list of broker addresses but can not know their ID yet.
        // Because of that, I have to check 2 lists, brokers with known ID and without (bootstrap ones)
        for addr in &self.bootstrap {
            match Broker::connect(*addr).await {
                Ok(broker) => {
                    match broker.send_request(&request).await {
                        Ok(resp) => return Ok(resp),
                        Err(e) => {info!("Error {}", e)},
                    }
                }
                Err(e) => info!("Error fetching topic meta: {}", e),
            }
        }


        Err(KafkaError::NoBrokerAvailable("Can not find broker to send request".to_owned()).into())
    }
}

/// Fetch metadata from broker. If retryable error happen, sleep and try again.
async fn fetch_topic_with_broker_and_retry(broker: &Broker, topics: &[&str]) -> Result<protocol::MetadataResponse0> {
    let req = protocol::MetadataRequest0 {
        topics: topics.iter().map(|t| t.to_string()).collect(),
    };
    broker.send_request(&req).await

    /*loop {
        let req = protocol::MetadataRequest0 {
            topics: topics.iter().map(|t| t.to_string()).collect(),
        };
        let meta = broker.send_request(&req).await?;
        let errors = meta.topics.iter().enumerate().
            filter_map(|(p, t)| match t {
                Err(t) => Some((p,t)),
                Ok(_) => None,
            }).
            collect::<Vec<_>>();
        // TODO: make retry more elegant
        // TODO: await on failed pat only and not on all of them
        if !errors.is_empty() {
            if errors.iter().all(|(_,e)|e.error_code.is_retriable()) {
                async_std::task::sleep(Duration::from_millis(500)).await;
                continue
            } else {
                let errors = errors.iter().map(|(p,t)| (*p,t.error_code)).collect();
                Err(Error::KafkaErrors(errors))?
            }
        } else {
            return Ok(meta);
        }
    }*/
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