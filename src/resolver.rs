use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::protocol::MetadataResponse0;
use std::collections::HashSet;

pub async fn start_resolver(mut requests: Receiver<String>, mut response: Sender<MetadataResponse0>) {
    let mut topics = HashSet::new();
    while let Some(topic) = requests.recv().await {
        if topics.insert(topic) {

        }
    }
}