//! # Design considerations
//!  
//!
//!
//!

use crate::broker::Broker;
use crate::cluster::Cluster;
use crate::error::KafkaError;
use crate::protocol;
use crate::protocol::Recordset;
use crate::types::{BrokerId, Partition};
use crate::utils;
use anyhow::Result;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::{debug_span, event, Level};
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use std::future::Future;

// TODO: offset start: -2, end: -1
pub enum StartOffset {
    Earliest,
    Latest,
}

#[derive(Debug)]
pub struct ConsumerBuilder {
    bootstrap: Option<String>,
    topic: String,
}

impl ConsumerBuilder {
    pub fn new(topic: impl AsRef<str>) -> Self {
        ConsumerBuilder {
            bootstrap: None,
            topic: topic.as_ref().to_string()
        }}

    pub fn bootstrap(mut self, bootstrap: &str) -> Self {
        self.bootstrap = Some(bootstrap.into());
        self
    }

    pub fn build(self) -> impl Future<Output = Result<Receiver<Batch>>> { Consumer::new(self) }
}

pub struct Batch {
    pub partition: u32,
    pub high_watermark: u64,
    pub messages: Vec<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Batch {
    fn form_recordset(partition: u32, high_watermark: u64, rs: Recordset) -> Self {
        Batch {
            partition,
            high_watermark,
            messages: rs
                .messages
                .into_iter()
                .map(|value| Message { key: vec![], value })
                .collect(),
        }
    }
}

// TODO: remove this struct
pub(crate) struct Consumer {
    config: ConsumerBuilder,
    cluster: Cluster,
    topic_meta: protocol::MetadataResponse0,
    partition_routing: HashMap<BrokerId, Vec<Partition>>,
}

impl Consumer {
    #[instrument]
    pub async fn new(builder: ConsumerBuilder) -> Result<Receiver<Batch>> {
        let bootstrap = builder.bootstrap.as_ref().map(|s| s.clone()).unwrap_or("localhost:9092".to_string());
        let seed_list = utils::resolve_addr(&bootstrap);
        debug!("Resolved bootstrap list: {:?}", seed_list);
        if seed_list.is_empty() {
            return Err(KafkaError::NoBrokerAvailable(format!(
                "Failed to resolve any address in bootstrap: {:?}",
                bootstrap
            )).into());
        }

        let mut cluster = Cluster::new(seed_list, None);
        let topic_meta = cluster.fetch_topic_meta(&[&builder.topic]).await?;
        debug!("Resolved topic: {:?}", topic_meta);
        assert_eq!(1, topic_meta.topics.len());

        let (tx, rx) = channel(1);

        tokio::spawn(async move {
            if let Err(error) = fetch_loop(cluster, tx, topic_meta, builder).await {
                error!("Fetch loop failed. Error: {}", error);
                event!(tracing::Level::ERROR, %error, "Fetch loop failed")
            }
        });
        Ok(rx)
    }
}

#[instrument(level = "debug", err, skip(cluster, tx, topic_meta, config))]
async fn fetch_loop(
    mut cluster: Cluster,
    tx: Sender<Batch>,
    topic_meta: protocol::MetadataResponse0,
    config: ConsumerBuilder,
) -> Result<()> {
    let mut offsets = vec![0_u64; topic_meta.topics[0].partition_metadata.len()];
    loop {
        // group partitions by leader broker
        let mut grouped_partitions: Vec<(u32, i32)> = topic_meta.topics[0]
            .partition_metadata
            .iter()
            .map(|p| (p.partition, p.leader))
            .collect();
        grouped_partitions.sort_by_key(|(_p, leader)| *leader);
        let grouped_partitions = grouped_partitions
            .into_iter()
            .group_by(|(_p, leader)| *leader);

        let fetch_requests: Vec<_> = grouped_partitions.into_iter().map(|(leader, partition_meta_group)| {
            let request = protocol::FetchRequest5 {
                replica_id: -1,
                max_wait_time: 1000,    // TODO: config
                min_bytes: 0,           // TODO: config
                max_bytes: 1_000_000,    // TODO: config
                isolation_level: 0,     // 0: READ_UNCOMMITTED; TODO: config, enum
                topics: vec![
                    protocol::FetchTopic {
                        // TODO: use ref
                        topic: config.topic.clone(),
                        // TODO: all partitions for now, make configurable in the future
                        // TODO: track position
                        partitions: partition_meta_group.into_iter().map(|(partition, _leader)| {
                            event!(Level::DEBUG, %leader, partition = %partition, fetch_offset = %offsets[partition as usize]);
                            protocol::FetchPartition {
                                partition,
                                fetch_offset: offsets[partition as usize] as i64,
                                log_start_offset: -1,
                                partition_max_bytes: 1_000_000,
                            }
                        }).collect()
                    }
                ]
            };
            (leader, request)
        }).collect();

        for (leader, request) in fetch_requests.into_iter() {
            event!(Level::DEBUG, %leader, "sending");

            let broker: Result<&Broker> = cluster.broker_get_or_connect(leader).await;
            let broker: &Broker = broker?;
            event!(Level::DEBUG, %leader, "got_broker");

            let response = broker.send_request(&request).await;
            event!(Level::INFO, %leader, "got response");
            match response {
                Ok(response) => {
                    if response.throttle_time != 0 {
                        debug!("Throttling: sleeping for {}ms", response.throttle_time);
                        tokio::time::sleep(Duration::from_millis(response.throttle_time as u64))
                            .await;
                    }
                    for response in response.responses {
                        // TODO: return topic in message
                        for fetch_partition in response.partitions {
                            let partition = fetch_partition.partition;
                            let recordset = fetch_partition.recordset;
                            //match fetch_partition.recordset {
                            //    Ok(recordset) => {
                            let last_offset = recordset.last_offset();
                            let dataset_size = recordset.messages.len();
                            if dataset_size > 0 {
                                debug!(
                                    "Partition#{} records[{}] offset {}",
                                    partition,
                                    recordset.messages.len(),
                                    recordset.base_offset
                                );
                                let batch = Batch::form_recordset(
                                    partition,
                                    fetch_partition.high_watermark,
                                    recordset,
                                );

                                let send_res = tx.send(batch)
                                    .instrument(debug_span!("Sending message", %leader, %partition, %last_offset, %dataset_size))
                                    .await;

                                if send_res.is_err() {
                                    info!("Listener closed. Exiting fetch loop");
                                    return Ok(());
                                }
                            }

                            // advance offset
                            if dataset_size > 0 {
                                offsets[partition as usize] = last_offset + 1;
                                debug!(
                                    "Advanced offset partition#{} to {}",
                                    partition,
                                    last_offset + 1
                                );
                            }
                        }
                    }
                }
                Err(_e) => {
                    // TODO: find error and log it. Check either it is recoverable.
                    debug!("Fetch failed");
                }
            }
        }

        // TODO: configurable fetch frequency
        tokio::time::sleep(Duration::from_millis(300))
            .instrument(debug_span!("Delay between fetches"))
            .await;
    }
}

/*
#[cfg(test)]
mod test {
    use super::*;
    use failure::Error;

    #[test]
    fn test() -> std::result::Result<(), failure::Error> {
        Ok(utils::init_test()?.block_on(async {
            let mut  consumer = Consumer::builder().
                bootstrap("127.0.0.1").
                // TODO: how to make topic mandatory?
                topic("test1".to_string()).
                build().await?;
            let msg = consumer.recv().await;
            debug!("Got msg {:?}", msg);
            Ok::<(),failure::Error>(())
        }).expect("Executed with error"))

        //Ok(())
    }
}
*/
