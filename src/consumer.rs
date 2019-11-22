use crate::cluster::Cluster;
use crate::protocol;
use derive_builder::Builder;
use crate::error::{Result, Error};
use failure::ResultExt;
use futures::Stream;
use async_std::sync::Receiver;
use std::collections::HashMap;
use crate::types::{BrokerId, Partition};

// TODO: offset start: -2, end: -1

#[derive(Default, Builder)]
#[builder(setter(into), default)]
pub struct ConsumerConfig {
    #[builder(default = "\"localhost\".to_string()")]
    pub bootstrap: String,
    pub topic: String
    // TODO: subscribe to certain partitions only
}

#[derive(Debug)]
pub struct Message {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct Consumer {
    config: ConsumerConfig,
    cluster: Cluster,
    topic_meta: protocol::MetadataResponse0,
    partition_routing: HashMap<BrokerId, Vec<Partition>>
}

impl Consumer {
    pub async fn new(config: ConsumerConfig) -> Result<Receiver<Message>> {
        let mut cluster = Cluster::connect(vec![config.bootstrap.clone()]).await.context("Consumer: new")?;
        let topic_meta = cluster.resolve_topic(&config.topic).await.context("Consumer:new:resolve topic")?;
        debug!("Resolved topic: {:?}", topic_meta);
        assert_eq!(1, topic_meta.topics.len());

        let mut partition_routing = HashMap::new();
        for p in &topic_meta.topics[0].partition_metadata {
            partition_routing.entry(p.leader).or_insert(vec![]).push(p.partition);
        }

        let mut consumer = Consumer {config, cluster, topic_meta, partition_routing};
        let (tx, rx) = async_std::sync::channel(1);

        tokio_current_thread::spawn(async move {
            for partition in &consumer.topic_meta.topics[0].partition_metadata {
                let broker_id = partition.leader;
                let broker = consumer.cluster.broker_by_id(broker_id).
                    expect("Can't find broker in metadata");


                for (broker_id, partitions) in &consumer.partition_routing {
                    let request = protocol::FetchRequest0 {
                        replica_id: -1,
                        max_wait_time: 1000,    // TODO: config
                        min_bytes: 0,           // TODO: config
                        max_bytes: 1000_000,    // TODO: config
                        isolation_level: 0,     // 0: READ_UNCOMMITTED; TODO: config, enum
                        topics: vec![
                            protocol::FetchTopic {
                                // TODO: use ref
                                topic: consumer.config.topic.clone(),
                                // TODO: all partitions for now, make configurable in the future
                                // TODO: track position
                                partitions: partitions.iter().map(|&partition| {
                                    protocol::FetchPartition {
                                        partition,
                                        fetch_offset: 0, //-1_i64,
                                        log_start_offset: -1,
                                        partition_max_bytes: 1000_000,
                                    }
                                }).collect()
                            }
                        ]
                    };
                    debug!("Fetch request: {:?}", request);
                    let response = broker.send_request(request).await;
                }
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        tokio_current_thread::block_on_all(async {
            simple_logger::init_with_level(log::Level::Debug)?;
            let config = ConsumerConfigBuilder::default().
                bootstrap("127.0.0.1:9092").
                // TODO: how to make topic mandatory?
                topic("test1").
                build().unwrap();
            let mut consumer = Consumer::new(config).await?;
            while let Some(msg) = consumer.recv().await {
                debug!("Got msg {:?}", msg);
            }

            Ok::<(), Error>(())
        }).expect("Executed with error");
    }
}