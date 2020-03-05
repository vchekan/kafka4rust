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

    pub async fn build(self) -> Result<Receiver<Message>> {
        Consumer::new(self).await
    }
}

#[derive(Debug)]
pub struct Message {
    pub partition: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// TODO: remove this struct
pub struct Consumer {
    config: ConsumerConfig,
    cluster: Cluster,
    topic_meta: protocol::MetadataResponse0,
    partition_routing: HashMap<BrokerId, Vec<Partition>>
}

impl Consumer {
    pub fn builder() -> ConsumerConfig { ConsumerConfig::new() }
    
    pub async fn new(config: ConsumerConfig) -> Result<Receiver<Message>> {
        let seed_list = utils::to_bootstrap_addr(&config.get_bootstrap());
        if seed_list.len() == 0 {
            return Err(From::from(format_err!("Failed to resolve any server from: '{}'", config.get_bootstrap()).
                context("Consumer resolve bootstrap")));
        }

        let mut cluster = Cluster::connect(seed_list).await.context("Consumer: new")?;
        let topic_meta = cluster.resolve_topic(&config.get_topic()?).await.context("Consumer:new:resolve topic")?;
        debug!("Resolved topic: {:?}", topic_meta);
        assert_eq!(1, topic_meta.topics.len());

        let mut partition_routing = HashMap::new();
        for p in &topic_meta.topics[0].partition_metadata {
            partition_routing.entry(p.leader).or_insert(vec![]).push(p.partition);
        }


        //let mut consumer = Consumer {config, cluster, topic_meta, partition_routing};
        let (mut tx, rx) = channel(1);

        tokio::spawn(async move {
            for partition in &topic_meta.topics[0].partition_metadata {
                let broker_id = partition.leader;
                let broker = cluster.broker_by_id(broker_id).
                    expect("Can't find broker in metadata");


                for (broker_id, partitions) in &partition_routing {
                    let topic = config.topic.as_ref().expect("Topic is missing").clone();
                    let request = protocol::FetchRequest5 {
                        replica_id: -1,
                        max_wait_time: 1000,    // TODO: config
                        min_bytes: 0,           // TODO: config
                        max_bytes: 1000_000,    // TODO: config
                        isolation_level: 0,     // 0: READ_UNCOMMITTED; TODO: config, enum
                        topics: vec![
                            protocol::FetchTopic {
                                // TODO: use ref
                                topic,
                                // TODO: all partitions for now, make configurable in the future
                                // TODO: track position
                                partitions: partitions.iter().map(|&partition| {
                                    protocol::FetchPartition {
                                        partition,
                                        fetch_offset: 0_i64,
                                        log_start_offset: -1,
                                        partition_max_bytes: 1000_000,
                                    }
                                }).collect()
                            }
                        ]
                    };
                    debug!("Fetch request: {:?}", request);
                    
                    match broker.send_request(&request).await {
                        Ok(response) => {
                            debug!("Fetched");
                            if response.throttle_time != 0 {
                                debug!("Throttling: sleeping for {}ms", response.throttle_time);
                                tokio::time::delay_for(Duration::from_millis(response.throttle_time as u64)).await;
                            }
                            for response in response.responses {
                                // TODO: return topic in message
                                for partition in response.partitions {
                                    let error_code: ErrorCode = partition.error_code.into();
                                    if error_code != ErrorCode::None {
                                        debug!("Partition error: {:?}", error_code);
                                        continue;
                                    }
                                    debug!("Partition error: {:?}", error_code);
                                    
                                    match partition.recordset {
                                        Ok(recordset) => {
                                            for record in recordset.messages {
                                                let msg = Message {
                                                    partition: partition.partition,
                                                    key: vec![],
                                                    value: record,
                                                };
                                                if let Err(_) = tx.send(msg).await {
                                                    info!("Closing fetch loop");
                                                    return Ok(());
                                                }
                                            }        
                                        }
                                        Err(e) => {
                                            error!("Error decoding recordset: {}", e);
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
                }
            }
            Ok(())
        });
        Ok(rx)
    }
}

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
