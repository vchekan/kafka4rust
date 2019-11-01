use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::protocol;
use futures::{
    future::ready,
    stream::{FuturesUnordered, StreamExt},
};
use std::io;
/// Design questions
/// Q: What is communication and ownership between Cluster and TopicResolver?
/// A: channels
///
/// Q: How TopicResolver communicates with Connection?
/// A: Connections is a shared resource, used by both, Topic Resolver and by Producer. Topic Resolver
/// can send requests either to connection or through Producer. If directly then it must be aware of
/// cluster configuration, i.e. list of servers. If through Producer then Producer should have
/// response routing capabilities. This will force us to reimplement it in Consumer. Another design
/// is metadata message bus.
///
/// Q: should topic resolution be a separate component or part of Cluster?
/// A: list of unresolved topics is owned by resolver spawn, thus it can not belong to Cluster.
///
/// Q: should connect upon initialization or first request?
/// A: initialization because it will improve startup time. But on the other hand,
/// if app waits for connection before sending, we achieved nothing. Also, it is better to connect
/// upon initialization for sake of Fail Fast principle.
///
/// Q: Should `Connection::request()` take `&self` or `&mut self`?
/// A: Problem with reading answer. If 2 requests were send immediately one after another, Kafka
/// will preserve order. But do we have guaranteed order of execution of 2 tasks reading responses?
/// I don't think so. So we need correlation_id based responses.
use std::iter::FromIterator;

#[derive(Debug)]
pub(crate) struct Cluster {
    bootstrap: Vec<String>,
    //response_tx: mpsc::UnboundedSender<EventOut>,
    //topic_meta: Arc<Mutex<Meta>>,
    connections: Vec<Broker>,
    //response_routes: VecDeque<(u32,RespondTo)>,
    //resolver_tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug)]
pub(crate) enum EventOut {
    ResolvedTopic(protocol::TopicMetadata),
}

#[derive(Debug)]
pub(crate) struct Meta {
    //topic_meta: HashMap<i32, protocol::TopicMetadata>
}

impl Cluster {
    /*
    pub async fn new(bootstrap: Vec<String>) -> Cluster //(mpsc::UnboundedSender<EventIn>, mpsc::UnboundedReceiver<EventOut>)
    {
        //bichannel::bichannel(|msg| )
        Cluster {
            bootstrap,
        }
        let (cluster_request, cluster_response) = BiChannel::<EventIn,EventOut>::unbounded();
        let (response_tx, response_rx) = mpsc::unbounded();

        let (resolver_tx, resolver_rx) = mpsc::unbounded();
        let topic_resolver_response = start_topic_resolver(resolver_rx, cluster_request.tx.clone(), spawner);
        let mut cluster = Cluster {
            bootstrap,
            response_tx,
            connections: Vec::new(),
            response_routes: VecDeque::new(),
            resolver_tx,
        };

        //
        // Cluster event loop
        //
        juliex::spawn(async move {
            cluster_response.rx.for_each(|e|{
                cluster.handle_event(e)
            }).await;
            debug!("Cluster event loop exited");
        }).unwrap();

        //response_rx
        cluster_request
    }
        */

    /// Connect to at least one broker successfully .
    pub async fn connect(bootstrap: Vec<String>) -> io::Result<Self> {
        let connect_futures = bootstrap
            .iter()
            // TODO: log bad addresses which did not parse
            .filter_map(|addr| addr.parse().ok())
            .map(Broker::connect);

        // TODO: use `fold` to preserve and report last error if no result
        /*let resolved = futures::stream::futures_unordered(connectFutures).
                filter_map(|f| future::ready(f.ok())).
                next();
        let broker = match resolved.await {
            Some(broker) => broker,
            // TODO: failure
            None => return Err(io::Error::from(io::ErrorKind::NotFound)),
        };*/

        let mut resolved =
            FuturesUnordered::from_iter(connect_futures).filter_map(|f| ready(f.ok()));
        let broker = match resolved.next().await {
            Some(broker) => broker,
            // TODO: failure
            None => return Err(io::Error::from(io::ErrorKind::NotFound)),
        };

        // TODO: move it to BrokerConnection
        debug!("Connected to {:?}", broker);

        let cluster = Cluster {
            bootstrap,
            connections: vec![broker],
        };

        //cluster.start_topic_resolver().await?;

        Ok(cluster)
    }

    pub(crate) async fn resolve_topic(&self, topic: &str) -> Result<protocol::MetadataResponse0> {
        for broker in &self.connections {
            let req = protocol::MetadataRequest0 {
                topics: vec![topic.into()],
            };
            let res = broker.request(&req).await?;
            return Ok(res);
        }

        // TODO: start recovery?
        Err(Error::NoBrokerAvailable)

        /*
        // TODO: how to implement recovery policy?
        self.connections.iter_mut().map(|conn| {
            let req = protocol::MetadataRequest0 {topics: vec![topic]};
            let resp = conn.request(req).await?;
        })
        */
    }
}

/*
fn handle_event(&mut self, e: EventIn) -> impl Future<Output=()> {
    match e {
        EventIn::ResolveTopic(topic) => {
            self.resolver_tx.unbounded_send(topic).unwrap();
            FuturesUnion2::F1(future::ready(()))
        },
        EventIn::TopicResolved(meta) => {
            self.response_tx.unbounded_send(EventOut::ResolvedTopic(meta)).expect("Send failed");
            FuturesUnion2::F1(future::ready(()))
        },
        EventIn::FetchTopicMetadata(topic, respond_to) => {
            FuturesUnion2::F2(self.get_any_or_connect().
                map(|conn| {
                    ()
                }))
        }
    }
}

fn get_any_or_connect(&mut self) -> impl Future<Output=&BrokerConnection> {
    // TODO: simple strategy for now, get first connection
    if self.connections.len() == 0 {
        // TODO: check result
        self.connect();
    }

    // After connect, we have at least one connection
    let conn = self.connections.get(0);
    future::ready(&conn)
}
*/

/*
fn start_topic_resolver(event_in: mpsc::UnboundedReceiver<String>, cluster_tx: mpsc::UnboundedSender<EventIn>, spawner: &mut LocalSpawner) -> mpsc::UnboundedReceiver<protocol::TopicMetadata> {
    let (response_tx, response_rx) = mpsc::unbounded();

    let timer = crate::timer::new(Duration::new(10, 0)).
        map(Either::Right);
    let event_in = event_in.map(Either::Left);
    let event_in = event_in.select(timer);

    let exec_loop = async move {
        let mut topic_list = HashSet::<String>::new();
        let mut topic_in_progress = HashSet::<String>::new();
        event_in.for_each(|topic_or_timer| {
            debug!("topic_resolver: {:?}", topic_or_timer);
            match topic_or_timer {
                Either::Left(topic) => {
                    debug!("Adding topic '{}' to resolver list. List len: {}", topic, topic_list.len());
                    if !topic_list.insert(topic) {
                        debug!("Topic is already in waiting list");
                    }
                },
                Either::Right(_) => {
                    debug!("Got timer tick");
                    let topics = topic_list.drain().collect::<Vec<_>>();
                    let topics2 = topics.clone();
                    topics.into_iter().for_each(|t| {topic_in_progress.insert(t);});
                    cluster_tx.unbounded_send(EventIn::FetchTopicMetadata(topics2, RespondTo::TopicResolver)).expect("Failed to send topic to be resolved");

                    // Q: should we detected timeout? Input buffers will be accumulated until
                    // we get a response, which means no topics are going to be resolved until
                    // response to the previous one is resolved?
                    // A: no, if resolved by the same server, we will have to wait for response
                    // anyway because kafka handle messages one at the time. We might achieve parallel
                    // resolve if round-robin resolution server, but we would still wait for
                    // fetch timeouts. If needed, we might want to keep connection just for metadata
                    // requests but that's overkill.
                    //let meta = await!(event);
                    //let conn = await!(get_connection());
                    //let meta = await!(get_meta(topic_list));
                    //debug!("Topic resolver: got meta: {:?}", meta);
                }
            }
            future::ready(())
        }).await;
        debug!("topic_resolver loop exited");
    };

    spawner.spawn(exec_loop).unwrap();

    response_rx
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use std::env;

    #[test]
    fn resolve() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();

        task::block_on(async {
            //let addr = vec!["127.0.0.1:9092".to_string()];
            let bootstrap = vec![
                "no.such.host.com:9092".to_string(),
                env::var("kafka-bootstrap").unwrap_or("127.0.0.1:9092".to_string()),
            ];

            //let (tx, rx) = mpsc::unbounded();
            let mut cluster = Cluster::connect(bootstrap).await.unwrap();
            let topic_meta = cluster.resolve_topic("test1").await.unwrap();
            debug!("Resolved topic: {:?}", topic_meta);

            //info!("Bootstrapped: {:?}", cluster);
            //cluster.tx.unbounded_send(EventIn::ResolveTopic("test1".to_string())).unwrap();
            //let res = dbg!(cluster.rx.next().await);
            //debug!("Got response");
        });
    }
}
