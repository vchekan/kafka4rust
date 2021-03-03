use crate::cluster::Cluster;
use crate::protocol::MetadataResponse0;
use anyhow::Result;
use std::time::Duration;

pub async fn get_topic_metadata(seed: &str, topic: &str) -> Result<MetadataResponse0> {
    let mut cluster = Cluster::with_bootstrap(seed, Some(Duration::from_secs(20)))?;
    cluster.fetch_topic_meta(&[topic]).await
}
