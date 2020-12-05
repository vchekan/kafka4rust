use anyhow::Result;
use crate::cluster::Cluster;
use crate::protocol::MetadataResponse0;

pub async fn get_topic_metadata(seed: &str, topic: &str) -> Result<MetadataResponse0> {
    let mut cluster = Cluster::with_bootstrap(seed)?;
    Ok(cluster.fetch_topic_meta(&[topic]).await?)
}