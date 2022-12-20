use tokio::sync::{mpsc, broadcast, Notify};
use std::collections::{HashSet, HashMap};
use log::{debug, warn, error};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use crate::{ClusterHandler, protocol};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;
use crate::connection::ConnectionHandle;
use tracing::instrument;

pub struct ResolverHandle {
    tx: mpsc::Sender<Msg>,
    pub listener: broadcast::Receiver<protocol::MetadataResponse0>,
    publisher: broadcast::Sender<protocol::MetadataResponse0>,
}

struct Resolver {
    rx: mpsc::Receiver<Msg>,
    tx: mpsc::Sender<Msg>,  // need to send Timer messages to self
    listener: broadcast::Sender<protocol::MetadataResponse0>,
    // is used to filter out "no change" events
    state: HashMap<String,Vec<protocol::PartitionMetadata>>,
    
    awaiting_resolve: HashMap<String, Vec<oneshot::Sender<Vec<protocol::PartitionMetadata>>>>,
    // known brokers and topics
    knownTopicsBrokers: TopicRecords,
    new_topic: Arc<Notify>
}

struct TopicRecords {
    // List of known brokers
    brokers: Vec<SocketAddr>,
    // Topics to be resolved
    topics: HashSet<String>
}

#[derive(Debug)]
enum Msg {
    StartResolving(Vec<String>),
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

    #[instrument(level="debug", skip(self))]
    pub async fn start_resolve(&self, topics: Vec<String>) {
        if let Err(e) = self.tx.send(Msg::StartResolving(topics)).await {
            error!("Resolver channel failed: {}", e);
        }
    }

    pub fn listener(&self) -> broadcast::Receiver<protocol::MetadataResponse0> {
        self.publisher.subscribe()
    }
}

impl Resolver {
    fn new(brokers: Vec<SocketAddr>, rx: mpsc::Receiver<Msg>, tx: mpsc::Sender<Msg>, listener: broadcast::Sender<protocol::MetadataResponse0>) -> Self {
        let mut resolver = Resolver {
            rx,
            tx,
            listener,
            state: HashMap::new(),
            awaiting_resolve: HashMap::new(),
            knownTopicsBrokers: TopicRecords{
                brokers,
                topics: HashSet::new()
            },
            new_topic: Arc::new(Notify::new())
        };

        // tokio::spawn(Self::resolver_loop(resolver.knownTopicsBrokers, resolver.tx.clone(), resolver.new_topic.clone()));

        resolver
    }

    #[instrument(skip(self))]
    async fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::StartResolving(topics) => {
                //let mut topics = self.topics.lock().unwrap();
                tracing::debug!("handle started");
                for topic in topics {
                    if self.knownTopicsBrokers.topics.insert(topic) {
                        self.new_topic.notify_one();
                        tracing::debug!("handle: new topic");
                    } else {
                        tracing::debug!("handle: existing topic");
                    }
                }
                tracing::debug!("handle finished");
            },
            Msg::Timer => {
                if self.awaiting_resolve.is_empty() {
                    return;
                }
                let topics = self.awaiting_resolve.keys().cloned().collect::<Vec<_>>();
                todo!()
                // let cluster = ClusterHandler::new(self.bootstrap)
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

    fn remove_topic(&mut self, topic: &String) {
        self.state.remove(topic);
        self.knownTopicsBrokers.topics.remove(topic);
    }


    // async fn resolver_loop(known_topics_brokers: TopicRecords, tx: mpsc::Sender<Msg>, new_topic: Arc<Notify>) {
    //     loop {
    //         let timeout = tokio::time::sleep(Duration::from_secs(5));
    //         tokio::select! {
    //             _ = new_topic.notified() => {
    //                 debug_span!("resolver: loop woke up: new_topic");
    //             }
    //             _ = timeout => {
    //                 debug_span!("resolver: loop woke up: timer");
    //             }
    //         };
    //
    //         let (topics, brokers) = {
    //             // Fight drop at the end of syntax block
    //             //let topic_records_g = topic_records.lock().unwrap();
    //             let topics = known_topics_brokers.topics.clone();
    //             let brokers = known_topics_brokers.brokers.clone();
    //             (topics, brokers)
    //         };
    //
    //         debug!("Resolver: fetching topics: {:?} brokers: {:?}", topics, brokers);
    //
    //         if !brokers.is_empty() {
    //             for broker in brokers {
    //                 let conn = ConnectionHandle::new(broker);
    //                 // TODO: config timeout
    //                 let timeout = Duration::from_secs(10);
    //                 debug!("Resolver: sending meta request");
    //                 match conn.fetch_topic_with_broker(topics.iter().cloned().collect(), timeout).await {
    //                     Ok(meta) => {
    //                         debug!("Resolver got meta");
    //                         if tx.send(Msg::ResolvedTopic(meta)).await.is_err() {
    //                             error!("Failed to send resolve message to self");
    //                         }
    //                     },
    //                     Err(e) => {
    //                         debug!("Resolver failed meta {:?}", e);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

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
    tracing::debug!("resolver:run started");

    // let mut timeout = tokio::time::interval(Duration::from_secs(5));
    // timeout.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        select! {
            _ = timeout => {
                tracing::debug!("resolver:run tick");
                //resolver.tx.send(Msg::Timer).await.unwrap();
                resolver.handle(Msg::Timer).await;
            }
            msg = resolver.rx.recv() => {
                tracing::debug!("resolver:run msg received");
                match msg {
                    Some(msg) => { resolver.handle(msg).await; }
                    None => { break }
                }
            }
        }
    }

    // while let Some(msg) = resolver.rx.recv().await {
    //     debug!("resolver:run got message: {:?}", msg);
    //     resolver.handle(msg).await;
    // }

    debug!("resolver:run finished");
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use crate::init_tracer;
    use super::*;
    use crate::resolver::ResolverHandle;
    use tracing_futures::Instrument;
    use tracing::debug_span;

    #[tokio::test]
    #[instrument]
    async fn resolver_test() {
        simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Debug).init().unwrap();
        let _tracer = init_tracer("resolver test").unwrap();
        async {
            tracing::debug!("test");

            let mut resolver = ResolverHandle::new(vec!["127.0.0.1:9092".parse().unwrap()]);
            resolver.start_resolve(vec!["test1".to_string()]).await;
            tracing::debug!("Resolver started");

            let response = tokio::time::timeout(Duration::from_secs(20), resolver.listener.recv().instrument(debug_span!("resolver.listener.recv"))).await.unwrap();
            //assert!(response.error_code.is_ok());
            println!(">>>{:?}", response);
        }.instrument(debug_span!("resolver span")).await;
    }
}