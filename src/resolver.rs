use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::select;
use crate::protocol::MetadataResponse0;
use std::collections::HashSet;
use std::time::Duration;
use log::{debug, error};
use crate::Cluster;
use tracing::field::debug;
use tokio::sync::RwLock;
use std::sync::Arc;

pub(crate) enum Cmd {
    ResolveTopic(String),
    ResolvedTopic(String),
}

pub(crate) fn start_resolver(mut requests: Receiver<Cmd>, result_tx: Sender<Vec<String>>) {
    let mut topics = HashSet::<String>::new();

    tokio::task::spawn(async move {
        loop {
            select! {
                // Listen to new topic resolve inquiry
                topic = requests.recv() => {
                    match topic {
                        Some(Cmd::ResolveTopic(topic)) => {
                            topics.insert(topic); //{
                                // debug!("Added {} topic to resolving list", topic);
                            // } else {
                                // debug!("Topic {} is already being resolved", topic)
                            // }
                        }
                        Some(Cmd::ResolvedTopic(topic)) => {
                            if topics.remove(&topic) {
                                debug!("removing resolved topic from resolver's list: {}", topic);
                            } else {
                                warn!("resolved to remove resolved topic which was not in the resolving list: {}", topic);
                            }
                        }
                        None => {
                            debug!("resolver channel closed, exiting resolver loop");
                            return;
                        }
                    }
                }

                // Attempt to fetch metadata periodically
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // let topics: Vec<&str> = topics.iter().map(|t| t.as_str()).collect::<Vec<_>>();
                    // let meta = match cluster.fetch_topic_meta_no_update(&topics).await {
                    //     Ok(meta) => meta,
                    //     Err(e) => {
                    //         debug!("start_resolver: Failed to get meta: {:?}", e);
                    //         continue;
                    //     }
                    // };
                    if let Err(e) = result_tx.send(topics.iter().cloned().collect()).await {
                        error!("No listener for resolver loop result. Exiting resolver loop");
                        return;
                    }
                }
            }
        }
    });
}