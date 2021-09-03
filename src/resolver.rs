use tokio::sync::{mpsc, broadcast};
use std::collections::{HashSet, HashMap};
use std::time::Duration;
use log::{debug, info, warn, error};
use std::net::SocketAddr;
use crate::connection::ConnectionHandle;
use crate::protocol;
use crate::error::{BrokerResult, BrokerFailureSource};
use tokio::sync::oneshot;

pub struct ResolverHandle {
    tx: mpsc::Sender<Msg>,
    pub listener: broadcast::Receiver<protocol::MetadataResponse0>,
    publisher: broadcast::Sender<protocol::MetadataResponse0>,
}

struct Resolver {
    rx: mpsc::Receiver<Msg>,
    tx: mpsc::Sender<Msg>,  // need to send Timer messages to self
    listener: broadcast::Sender<protocol::MetadataResponse0>,
    brokers: Vec<SocketAddr>,
    topics: HashSet<String>,
    // used to filter out "no change" events
    state: HashMap<String,Vec<protocol::PartitionMetadata>>,
    awaiting_resolve: HashMap<String, Vec<oneshot::Sender<Vec<protocol::PartitionMetadata>>>>,
}

#[derive(Debug)]
enum Msg {
    StartResolving(String),
    ResolvedTopic(protocol::MetadataResponse0),
    Resolve(String, oneshot::Sender<Vec<protocol::PartitionMetadata>>),
    Timer,
}

impl ResolverHandle {
    pub fn new(brokers: Vec<SocketAddr>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let (listener_tx, listener) = broadcast::channel(1);
        let resolver = Resolver::new(brokers, rx, tx.clone(), listener_tx.clone());
        tokio::spawn(run(resolver));
        ResolverHandle { tx, listener , publisher: listener_tx}
    }

    pub async fn start_resolve(&self, topic: String) {
        if let Err(e) = self.tx.send(Msg::StartResolving(topic)).await {
            error!("Resolver channel failed: {}", e);
        }
    }

    pub fn listener(&self) -> broadcast::Receiver<protocol::MetadataResponse0> {
        self.publisher.subscribe()
    }
}

impl Resolver {
    fn new(brokers: Vec<SocketAddr>, rx: mpsc::Receiver<Msg>, tx: mpsc::Sender<Msg>, listener: broadcast::Sender<protocol::MetadataResponse0>) -> Self {
        Resolver {
            rx,
            tx,
            listener,
            brokers,
            topics: HashSet::new(),
            state: HashMap::new(),
            awaiting_resolve: HashMap::new(),
        }
    }

    async fn handle(&mut self, msg: Msg) {
        debug!("resolver:handle()");
        match msg {
            Msg::StartResolving(topic) => self.add_topic(topic).await,
            Msg::Timer => {
                let topics = self.topics.clone();
                let brokers = self.brokers.clone();
                debug!("Resolver: timer: topics: {:?} brokers: {:?}", topics, brokers);
                let tx = self.tx.clone();

                tokio::spawn(async move {
                    for broker in brokers {
                        let conn = ConnectionHandle::new(broker);
                        // TODO: config timeout
                        let timeout = Duration::from_secs(10);
                        debug!("Resolver: sending meta request");
                        match conn.fetch_topic_with_broker(topics.iter().cloned().collect(), timeout).await {
                            Ok(meta) => {
                                debug!("Resolver got meta");
                                if tx.send(Msg::ResolvedTopic(meta)).await.is_err() {
                                    error!("Failed to send resolve message to self");
                                }
                            },
                            Err(e) => {
                                debug!("Resolver failed meta {}", e);
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                if tx.send(Msg::Timer).await.is_err() {
                                    info!("Self channel closed. Exiting resolver loop");
                                    return;
                                }
                            }
                        }
                    }
                });
            }
            Msg::ResolvedTopic(meta) => {
                debug!("Resolver handles meta");
                // TODO: update known brokers
                for topic in &meta.topics {
                    if topic.error_code.is_ok() {
                        let (ok_partitions, err_partitions): (Vec<_>, Vec<_>) = topic.partition_metadata.iter().partition(|p| p.error_code.is_ok());
                        if err_partitions.is_empty() {
                            debug!("Resolved topic, removing from resolver: '{}'", topic.topic);
                            self.remove_topic(&topic.topic);
                            // self.fire_if_changed(topic).await;
                        }
                    } else if topic.error_code.is_retriable() {
                        // self.fire_if_changed(topic).await;
                        debug!("Topic error, will retry: {}", topic.error_code);
                    } else {
                        warn!("Non-resolvable error code, removing topic from resolver: '{}'", topic.topic);
                        self.remove_topic(&topic.topic);
                    }
                }
                match self.listener.send(meta) {
                    Ok(_) => debug!("Resolver: broadcasted meta"),
                    Err(e) => warn!("Failed to broadcast meta: {}", e)
                }


            }
            Msg::Resolve(topic, send_to) => {
                if let Some(meta) = self.state.get(&topic) {
                    send_to.send(meta.clone());
                } else {
                    self.awaiting_resolve.entry(topic).or_default().push(send_to)
                }
            }
        }
    }

    async fn add_topic(&mut self, topic: String) {
        self.topics.insert(topic);
        self.tx.send(Msg::Timer).await;
    }

    fn remove_topic(&mut self, topic: &String) {
        self.state.remove(topic);
        self.topics.remove(topic);
    }

    // async fn fire_if_changed(&mut self, meta: protocol::TopicMetadata) {
    //     let mut fire = false;
    //
    //     match self.state.get_mut(&meta.topic) {
    //         Some(partitions) => {
    //             if (partitions.len() != meta.partition_metadata.len()) {
    //                 fire = true;
    //             } else if partitions.iter().zip(meta.partition_metadata.iter()).any(|(a, b)| { a.error_code != b.error_code || a.partition != b.partition || a.leader != b.leader }) {
    //                 fire = true
    //             }
    //             if fire {
    //                 partitions.clear();
    //                 partitions.extend_from_slice(&meta.partition_metadata[..])
    //             }
    //         }
    //         None => {
    //             self.state.insert(meta.topic.clone(), meta.partition_metadata.clone());
    //             fire = true;
    //         }
    //     }
    //     if fire {
    //         if self.listener.send(Ok(meta)).await.is_err() {
    //             error!("Resolver: failed to senf metadata update to listener")
    //         }
    //     }
    // }
}

async fn run(mut resolver: Resolver) {
    debug!("resolver:run started");
    while let Some(msg) = resolver.rx.recv().await {
        debug!("resolver:run got message: {:?}", msg);
        resolver.handle(msg).await;
    }

    debug!("resolver:run finished");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::ResolverHandle;

    #[tokio::test]
    async fn test() {
        simple_logger::SimpleLogger::from_env().with_level(log::LevelFilter::Debug).init().unwrap();
        debug!("test");

        // let (bus_tx, mut bus_rx) = tokio::sync::broadcast::channel(1);

        let mut resolver = ResolverHandle::new(vec!["127.0.0.1:9092".parse().unwrap()]);
        resolver.start_resolve("test1".to_string()).await;

        let response = resolver.listener.recv().await.unwrap();
        //assert!(response.error_code.is_ok());
        println!(">>>{:?}", response);
    }
}