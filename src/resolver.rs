use tokio::sync::{mpsc, broadcast, Notify};
use std::collections::{HashSet, HashMap};
use log::{debug, warn, error};
use std::net::SocketAddr;
use std::sync::Arc;
use crate::protocol;
use tokio::sync::oneshot;
use tracing_attributes::instrument;

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
    //topics: Arc<Mutex<TopicRecords>>,
    // known brokers and topics
    topics: TopicRecords,
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
            topics: TopicRecords{
                brokers,
                topics: HashSet::new()
            },
            new_topic: Arc::new(Notify::new())
        };

        //Self::start_resolve_loop(resolver.topics, resolver.tx.clone(), resolver.new_topic.clone());

        resolver
    }

    #[instrument(skip(self))]
    async fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::StartResolving(topics) => {
                //let mut topics = self.topics.lock().unwrap();
                for topic in topics {
                    if self.topics.topics.insert(topic) {
                        self.new_topic.notify_one();
                    }
                }
            },
            Msg::Timer => {
                let tx = self.tx.clone();
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
        self.topics.topics.remove(topic);
    }

    // fn start_resolve_loop(topic_records: TopicRecords, tx: mpsc::Sender<Msg>, new_topic: Arc<Notify>) {
    //     tokio::spawn(async move {
    //         loop {
    //             let timeout = tokio::time::sleep(Duration::from_secs(5));
    //             tokio::select! {
    //                 _ = new_topic.notified() => {
    //                     debug!("resolver: loop woke up: new_topic");
    //                 }
    //                 _ = timeout => {
    //                     debug!("resolver: loop woke up: timer");
    //                 }
    //             };
    //
    //             let (topics, brokers) = {
    //                 // Fight drop at the end of syntax block
    //                 //let topic_records_g = topic_records.lock().unwrap();
    //                 let topics = topic_records.topics.clone();
    //                 let brokers = topic_records.brokers.clone();
    //                 (topics, brokers)
    //             };
    //
    //             debug!("Resolver: fetching topics: {:?} brokers: {:?}", topics, brokers);
    //
    //             if !brokers.is_empty() {
    //                 for broker in brokers {
    //                     let conn = ConnectionHandle::new(broker);
    //                     // TODO: config timeout
    //                     let timeout = Duration::from_secs(10);
    //                     debug!("Resolver: sending meta request");
    //                     match conn.fetch_topic_with_broker(topics.iter().cloned().collect(), timeout).await {
    //                         Ok(meta) => {
    //                             debug!("Resolver got meta");
    //                             if tx.send(Msg::ResolvedTopic(meta)).await.is_err() {
    //                                 error!("Failed to send resolve message to self");
    //                             }
    //                         },
    //                         Err(e) => {
    //                             debug!("Resolver failed meta {:?}", e);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }.instrument(debug_span!("resolver_thread")));
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
        simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Debug).init().unwrap();
        debug!("test");

        // let (bus_tx, mut bus_rx) = tokio::sync::broadcast::channel(1);

        let mut resolver = ResolverHandle::new(vec!["127.0.0.1:9092".parse().unwrap()]);
        resolver.start_resolve(vec!["test1".to_string()]).await;


        let response = resolver.listener.recv().await.unwrap();
        //assert!(response.error_code.is_ok());
        println!(">>>{:?}", response);

        //tokio::time::sleep(Duration::from_secs(15)).await;
    }
}