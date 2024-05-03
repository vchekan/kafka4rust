use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use futures::TryStream;
use crate::connection::BrokerConnection;
use crate::protocol;
use async_stream::try_stream;
use crate::error::{BrokerFailureSource, BrokerResult};
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, instrument, Instrument};
use futures::future::Fuse;
use futures_lite::pin;
use futures_util::FutureExt;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;
use futures_lite::StreamExt;

#[derive(Debug)]
pub(crate) struct MetaDiscover {
    state: State,
    bootstrap: Vec<SocketAddr>,
    broker_idx: usize,
    topic_requests: mpsc::Receiver<String>,
}

#[derive(PartialEq, Debug)]
enum State {
    Disconnected,
    Connected,
    Connecting,
    RetrySleep,
    FetchingMeta,
}

struct RequestFuture<F> {
    conn: BrokerConnection,
    exchange: F,
}

impl MetaDiscover {
    #[instrument(level = "debug", name = "MetaDiscover::new", skip(bootstrap))]
    pub fn new(bootstrap: Vec<SocketAddr>) -> (Sender<String>, Receiver<Result<protocol::MetadataResponse0,BrokerFailureSource>>) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);

        let discover = MetaDiscover {
            state: State::Disconnected,
            broker_idx: 0,
            bootstrap,
            topic_requests: command_rx,
        }.stream();

        tokio::task::spawn(async move {
            pin!(discover);
            loop {
                match discover.next().await {
                    Some(Ok(meta)) => {
                        if let Err(_) = response_tx.send(Ok(meta)).await {
                            break;
                        }
                    },
                    Some(Err(e)) => {
                        if let Err(e) = response_tx.send(Err(e)).await {
                            tracing::debug!("Discover consumer closed channel, exiting");
                            break;
                        }
                    },
                    None => {
                        tracing::debug!("Discover stream closed, exiting the loop");
                        break;
                    }
                }
            }

            tracing::debug!("MetaDiscover loop exited");
        }.instrument(debug_span!("meta discover loop")));

        (command_tx, response_rx)
    }

    async fn eval_loop() {
        
    }

    #[instrument()]
    fn stream(mut self) -> impl TryStream<Ok = protocol::MetadataResponse0, Error = BrokerFailureSource, Item=Result<protocol::MetadataResponse0,BrokerFailureSource>> {
        let stream = try_stream! {
            let mut topics = HashSet::new();
            let timeout = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(timeout);
            let mut connected_broker = None;
            let fetching_meta = Fuse::terminated();
            tokio::pin!(fetching_meta);

            'outer: loop {
                if self.broker_idx >= self.bootstrap.len() {
                    self.broker_idx = 0;
                }
                tracing::debug!("Awaiting in state: {:?}, topics[{}]", self.state, topics.len());
                select! {
                    // Even disconnected, keep accepting new topics to be resolved
                    topic = self.topic_requests.recv() => {
                        match topic {
                            Some(topic) => {
                                tracing::debug!("Topic to be resolved: {topic}");
                                if topics.insert(topic) {
                                    tracing::debug!("Added topic to be resolved");
                                } else {
                                    tracing::debug!("Topic is alresy being resolved");
                                }
                                if let State::Connected = self.state {
                                    tracing::debug!("Starting fetching");
                                    let topics = topics.iter().cloned().collect();
                                    fetching_meta.set(Self::fetching_meta(topics, connected_broker.take().unwrap()).fuse());
                                    self.state = State::FetchingMeta;
                                }
                            },
                            None => {
                                tracing::debug!("Topics input stream closed, exiting discovery loop");
                                break 'outer
                            }
                        }
                    }

                    // Always try to connect when disconnected
                    conn = BrokerConnection::connect(self.bootstrap[self.broker_idx], 0), if self.state == State::Disconnected => {
                        match conn {
                            Ok(conn) => {
                                tracing::debug!("Connected");
                                if topics.is_empty() {
                                    connected_broker.insert(conn);
                                } else {
                                    let topics = topics.iter().cloned().collect();
                                    debug!("Activated fetching_meta");
                                    fetching_meta.set(Self::fetching_meta(topics, conn).fuse());
                                }
                                self.state = State::Connected;
                                self.broker_idx += 1;
                            }
                            Err(e) => {
                                tracing::debug!("Failed to connect, will try again: {e}");
                                timeout.as_mut().reset(Instant::now() + Duration::from_secs(5));
                                self.state = State::RetrySleep;
                            }
                        }
                    }

                    //
                    (meta, conn) = &mut fetching_meta =>
                    {
                        match meta {
                            Ok(meta) => {
                                debug!("Resolved successfully. Topics[{}]", meta.topics.len());
                                self.state = State::Connected;

                                for topic in &meta.topics {
                                    if topic.error_code.is_ok() {
                                        topics.remove(&topic.topic);
                                        debug!("Resolved topic {}", topic.topic);
                                    }
                                }

                                if !topics.is_empty() {
                                    timeout.as_mut().reset(Instant::now() + Duration::from_secs(5));
                                    self.state = State::RetrySleep;
                                }
                                let _ = connected_broker.replace(conn);

                                yield(meta);
                            }
                            Err(e) => info!("Error fetching topics metadata: {e}")
                        }
                    }

                    _ = &mut timeout, if self.state == State::RetrySleep => {
                        self.state = State::Disconnected;
                    }
                }
            }
        };
        stream
    }

    async fn fetching_meta(topics: Vec<String>, mut conn: BrokerConnection) -> (BrokerResult<protocol::MetadataResponse0>, BrokerConnection) {
        let req = protocol::MetadataRequest0 { topics };
        let res = conn.exchange(&req).await;
        (res, conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use tracing::{debug_span, info_span, Instrument};
    use crate::init_tracer;
    use crate::utils::resolve_addr;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        init_tracer("test");
        let span = info_span!("test");
        let _guard = span.enter();

        timeout(Duration::from_secs(20), async move {

            let bootstrap = resolve_addr("localhost");
            let (discover_tx, mut discover_rx) = MetaDiscover::new(bootstrap);
            let topics = vec!["topic1", "topic2", "topic3"];
            let topic_count = topics.len();
            tokio::task::spawn(async move {
                for topic in topics {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    debug!("Sending topic for resolution: {topic}");
                    discover_tx.send(topic.to_string()).await.unwrap();
                    debug!("Sent");
                }
            }.instrument(debug_span!("send loop")));

            let mut broker_resolved = 0;
            loop {
                match discover_rx.recv().await {
                    Some(Ok(conn)) => {
                        println!("> {conn:?}");
                        broker_resolved += 1;
                        if broker_resolved == topic_count {
                            break;
                        }
                    },
                    Some(Err(e)) => println!("Error: {e}"),
                    None => {
                        println!("EOS");
                        break;
                    }
                }
            }
        }).await?;
        Ok(())
    }
}