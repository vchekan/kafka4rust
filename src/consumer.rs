//! # Design considerations
//!  
//!
//!
//!

use crate::cluster::Cluster;
use crate::protocol;
use crate::error::{Result, Error};
use crate::types::{BrokerId, Partition};
use crate::utils;
use failure::{ResultExt, format_err};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver};
use protocol::ErrorCode;
use itertools::Itertools;
use tracing::{span, debug_span, event, Span, Level};
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use crate::protocol::Recordset;

// TODO: offset start: -2, end: -1
pub enum StartOffset {
    Earliest,
    Latest
}

pub struct ConsumerConfig {
    bootstrap: Option<String>,
    topic: Option<String>,
}

impl ConsumerConfig {
    fn new() -> ConsumerConfig {
        ConsumerConfig {
            bootstrap: None,
            topic: None,
        }
    }

    pub fn bootstrap(mut self, bootstrap: &str) -> Self {
        self.bootstrap = Some(bootstrap.to_string());
        self
    }
    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    fn get_bootstrap(&self) -> &str {
        match &self.bootstrap {
            Some(b) => b.as_str(),
            None => "localhost"
        }
    }

    fn get_topic(&self) -> Result<&str> {
        match &self.topic {
            Some(t) => Ok(t.as_str()),
            None => Err(Error::Config("Consumer topic is not set".to_string()))
        }
    }

    pub async fn build(self) -> Result<Receiver<Batch>> {
        Consumer::new(self).await
    }
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
            messages: rs.messages.into_iter().map(|value|
                Message {key: vec![], value}
            ).collect()
        }
    }
}

// TODO: remove this struct
pub struct Consumer {
    config: ConsumerConfig,
    cluster: Cluster,
    topic_meta: protocol::MetadataResponse0,
    partition_routing: HashMap<BrokerId, Vec<Partition>>,
}

impl Consumer {
    pub fn builder() -> ConsumerConfig { ConsumerConfig::new() }

    #[instrument(skip(config))]
    pub async fn new(config: ConsumerConfig) -> Result<Receiver<Batch>> {
        let seed_list = utils::to_bootstrap_addr(&config.get_bootstrap());
        if seed_list.len() == 0 {
            return Err(From::from(format_err!("Failed to resolve any server from: '{}'", config.get_bootstrap()).
                context("Consumer resolve bootstrap")));
        }

        let mut cluster = Cluster::new(seed_list).context("Consumer: new")?;
        let topic_meta = cluster.fetch_topic_meta(&vec![config.get_topic()?]).await.context("Consumer:new:resolve topic")?;
        debug!("Resolved topic: {:?}", topic_meta);
        assert_eq!(1, topic_meta.topics.len());

        let (mut tx, rx) = channel(1);

        tokio::spawn(async move {
            let loop_span = debug_span!("fetch_loop");
            let loop_guard = loop_span.enter();
            let mut offsets = vec![0_u64; topic_meta.topics[0].partition_metadata.len()];
            loop {
                let build_fetch_req_span = debug_span!(parent: &loop_span, "build fetch");
                let _build_fetch_req_span_guard = build_fetch_req_span.enter();
                // group partitions by leader broker
                let mut grouped_partitions: Vec<(u32, i32)> = topic_meta.topics[0].partition_metadata.iter().map(|p| (p.partition, p.leader)).collect();
                grouped_partitions.sort_by_key(|(p, leader)| *leader);
                let grouped_partitions = grouped_partitions.into_iter().group_by(|(p, leader)| *leader);

                let fetch_requests: Vec<_> = grouped_partitions.into_iter().map(|(leader, partition_meta_group)| {
                    let request = protocol::FetchRequest5 {
                        replica_id: -1,
                        max_wait_time: 1000,    // TODO: config
                        min_bytes: 0,           // TODO: config
                        max_bytes: 1000_000,    // TODO: config
                        isolation_level: 0,     // 0: READ_UNCOMMITTED; TODO: config, enum
                        topics: vec![
                            protocol::FetchTopic {
                                // TODO: use ref
                                topic: config.topic.as_ref().expect("Topic is missing").clone(),
                                // TODO: all partitions for now, make configurable in the future
                                // TODO: track position
                                partitions: partition_meta_group.into_iter().map(|(partition, _leader)| {
                                    event!(Level::DEBUG, %leader, partition = %partition, fetch_offset = %offsets[partition as usize]);
                                    protocol::FetchPartition {
                                        partition,
                                        fetch_offset: offsets[partition as usize] as i64,
                                        log_start_offset: -1,
                                        partition_max_bytes: 1000_000,
                                    }
                                }).collect()
                            }
                        ]
                    };
                    (leader, request)
                }).collect();
                std::mem::drop(_build_fetch_req_span_guard);
                std::mem::drop(build_fetch_req_span);

                for (leader, request) in fetch_requests.into_iter() {
                    let fetch_req_span = debug_span!("fetch_request", %leader);
                    let fetch_req_span_guard = fetch_req_span.enter();
                    event!(Level::DEBUG, %leader, "sending");

                    let broker = cluster.broker_get_or_connect(leader).await?;
                    event!(Level::DEBUG, %leader, "got_broker");

                    let response = broker.send_request(&request).await;
                    event!(Level::INFO, %leader, "got response");
                    match response {
                        Ok(response) => {
                            if response.throttle_time != 0 {
                                debug!("Throttling: sleeping for {}ms", response.throttle_time);
                                tokio::time::delay_for(Duration::from_millis(response.throttle_time as u64)).await;
                            }
                            for response in response.responses {
                                // TODO: return topic in message
                                for partition in response.partitions {
                                    let error_code: ErrorCode = partition.error_code.into();
                                    if error_code != ErrorCode::None {
                                        debug!("Partition {} error: {:?}", partition.partition, error_code);
                                        event!(Level::INFO, %leader, partition = %partition.partition, ?error_code, "Partition error");
                                        continue;
                                    }

                                    match partition.recordset {
                                        Ok(recordset) => {
                                            let last_offset = recordset.last_offset();
                                            let dataset_size = recordset.messages.len();
                                            if dataset_size > 0 {
                                                debug!("Partition#{} records[{}] offset {}", partition.partition, recordset.messages.len(), recordset.base_offset);
                                                let batch = Batch::form_recordset(partition.partition, partition.high_watermark, recordset);

                                                let send_res = tx.send(batch)
                                                    .instrument(debug_span!("Sending message", %leader, partition = %partition.partition, %last_offset, %dataset_size))
                                                    .await;

                                                if let Err(_) = send_res {
                                                    info!("Listener closed. Exiting fetch loop");
                                                    return Ok(());
                                                }
                                            }

                                            // advance offset
                                            if dataset_size > 0 {
                                                offsets[partition.partition as usize] = last_offset + 1;
                                                debug!("Advanced offset partition#{} to {}", partition.partition, last_offset + 1);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Error decoding recordset: {}", e);
                                            event!(Level::ERROR, %leader, partition = %partition.partition, error = %e, "Error decoding recordset");
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            // TODO:
                            debug!("Fetch failed");
                        }
                    }
                    std::mem::drop(fetch_req_span_guard);
                    std::mem::drop(fetch_req_span);
                }


                let delay_span = debug_span!("Delay between responses");
                let delay_span_guard = delay_span.enter();
                // TODO: configurable fetch frequency
                tokio::time::delay_for(Duration::from_millis(300 as u64)).await;
                std::mem::drop(delay_span_guard);
                std::mem::drop(delay_span);
            }
            debug!("Exiting fetch loop");
        });
        Ok(rx)
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