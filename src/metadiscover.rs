use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::TryStream;
use crate::connection::BrokerConnection;
use crate::utils::resolve_addr;
use crate::protocol;
use crate::types::BrokerId;
use async_stream::try_stream;
use tower::discover::Change;
use crate::error::{BrokerFailureSource, BrokerResult};
use tokio::select;
use tokio::sync::mpsc;
use tracing::info;
use futures::future::Fuse;
use futures_lite::pin;
use futures_util::FutureExt;

pub(crate) struct MetaDiscover {
    state: State,
    bootstrap: Vec<SocketAddr>,
    broker_idx: usize,
    // conn: Option<BrokerConnection>,
    topic_requests: mpsc::Receiver<String>,
}

#[derive(PartialEq, Debug)]
enum State {
    Disconnected,
    Connected,
    Connecting,
    FetchingMeta,
}

// struct OptionBrokerConnection(Option<BrokerConnection>);

// pin_project! {
struct RequestFuture<F> {
    //#[pin]
    conn: BrokerConnection,
    exchange: F,
    //req: R,
}
// }

impl MetaDiscover {
    pub fn new(bootstrap: &str, topic_requests: mpsc::Receiver<String>) -> Self {
        MetaDiscover {
            state: State::Disconnected,
            broker_idx: 0,
            bootstrap: resolve_addr(bootstrap),
            // conn: None,
            topic_requests,
        }
    }
    fn stream(&mut self) -> impl TryStream<Ok = Change<BrokerId,protocol::Broker>, Error = BrokerFailureSource, Item=Result<Change<BrokerId,protocol::Broker>,BrokerFailureSource>> + '_ {

        let stream = try_stream! {
            let mut topics = HashSet::new();
            // let timeout = tokio::time::sleep(Duration::from_secs(10));
            // tokio::pin!(timeout);
            let mut connected_broker = None;
            let fetching_meta = Fuse::terminated();
            tokio::pin!(fetching_meta);

            'outer: loop {
                    if self.broker_idx >= self.bootstrap.len() {
                        self.broker_idx = 0;
                    }
                    tracing::debug!("Awaiting in state: {:?}", self.state);
                    select! {
                        // Even disconnected, keep accepting new topics to be resolved
                        topic = self.topic_requests.recv() => {
                            match topic {
                                Some(topic) => {
                                    tracing::debug!("Added topic to be resolved: {topic}");
                                    topics.insert(topic);
                                    if let State::Connected = self.state {
                                        tracing::debug!("Starting fetching");
                                        let topics = topics.iter().cloned().collect();
                                        fetching_meta.set(Self::fetching_meta(topics, connected_broker.take().unwrap()).fuse());
                                        self.state = State::FetchingMeta;
                                    }
                                },
                                None => {
                                    tracing::debug!("Topics input stram closed, exiting discovery loop");
                                    break 'outer
                                }
                            }
                        }

                        // Always try to connect when disconnected
                        conn = BrokerConnection::connect(self.bootstrap[self.broker_idx]), if self.state == State::Disconnected => {
                            match conn {
                                Ok(conn) => {
                                    tracing::debug!("Connected");
                                    connected_broker.insert(conn);
                                    self.state = State::Connected;
                                    self.broker_idx += 1;
                                    continue;
                                }
                                Err(e) => {
                                    tracing::debug!("Failed to connect, will try again: {e}");
                                }
                            }
                        }

                        //
                        (meta, conn) = &mut fetching_meta =>
                        {
                            match meta {
                                Ok(meta) => {
                                    self.state = State::Connected;
                                    for broker in meta.brokers {
                                        yield(Change::Insert(broker.node_id, broker));
                                    }
                                }
                                Err(e) => info!("Error fetching topics metadata: {e}")
                            }

                        }
                        // _ = &mut timeout, if !topics.is_empty() => {
                        //
                        // }
                    }
            }
        };
        stream
    }

    async fn fetching_meta(topics: Vec<String>, mut conn: BrokerConnection) -> (BrokerResult<protocol::MetadataResponse0>, BrokerConnection) {
        //let topics = topics.iter().cloned().collect();
        let req = protocol::MetadataRequest0 { topics };
        let res = conn.exchange(req).await;
        (res, conn)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use futures::pin_mut;
    use super::*;
    use tokio::time::timeout;
    use futures_lite::stream::StreamExt;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        simple_logger::SimpleLogger::new().env().with_level(log::LevelFilter::Debug).init().unwrap();

        timeout(Duration::from_secs(10), async move {
            let _trace = crate::utils::init_tracer("metadiscover-test").unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let tx2 = tx.clone();   // to keep it open until end of test
            let mut discover = MetaDiscover::new("localhost", rx);
            let stream = discover.stream();
            pin_mut!(stream);
            tokio::task::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                tx.send("topic1".to_string()).await.unwrap();
            });

            let mut broker_resolved = 0;
            loop {
                match stream.next().await {
                    Some(Ok(conn)) => {
                        println!("> {conn:?}");
                        broker_resolved += 1;
                        if broker_resolved == 3 {
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
        }).await.unwrap();
        Ok(())
    }
}