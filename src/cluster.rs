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
/// A: list of unresolved topics is owned by resolver spawn, thus it can non belong to Cluster.
///
/// Q: should connect upon initialization or first request?
/// A: initialization because it will improve startup time. But on the other hand,
/// if app waits for connection before sending, we achieved nothing.
///

use crate::broker::Broker;
use futures::{future::{self, Future},
              channel::mpsc,
              executor::LocalSpawner,
              task::SpawnExt,
              FutureExt};
use std::io;
use crate::protocol;
use std::collections::HashSet;
use std::time::Duration;
use futures::StreamExt;
use crate::bichannel::BiChannel;
use crate::connection::BrokerConnection;
use std::collections::vec_deque::VecDeque;
use either::Either;
use crate::futures_union::FuturesUnion2;

#[derive(Debug)]
pub(crate) struct Cluster {
    bootstrap: Vec<String>,
    response_tx: mpsc::UnboundedSender<EventOut>,
    //topic_meta: Arc<Mutex<Meta>>,
    connections: Vec<BrokerConnection>,
    response_routes: VecDeque<(u32,RespondTo)>,
    resolver_tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug)]
pub(crate) enum EventIn {
    ResolveTopic(String),
    TopicResolved(protocol::TopicMetadata),
    // TODO: make this member private (move to internal events enum?)
    FetchTopicMetadata(Vec<String>, RespondTo),
}

#[derive(Debug)]
pub (crate) enum EventOut {
    ResolvedTopic(protocol::TopicMetadata)
}

#[derive(Debug)]
pub(crate) struct Meta {
    //topic_meta: HashMap<i32, protocol::TopicMetadata>
}

/// When response from kafka comes back, whom to route response to
/// (who requested the info).
#[derive(Debug)]
pub(crate) enum RespondTo {
    Out,
    TopicResolver,
}

impl Cluster {
    pub fn new(bootstrap: Vec<String>, /*event_in: mpsc::UnboundedReceiver<EventIn>,*/ spawner: &mut LocalSpawner) ->
        //mpsc::UnboundedReceiver<EventOut>
        BiChannel<EventIn,EventOut>
    {
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
        spawner.spawn(async move {
            await!(cluster_response.rx.for_each(|e|{
                cluster.handle_event(e)
            }));
            debug!("Cluster event loop exited");
        }).unwrap();

        //response_rx
        cluster_request
    }

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

/*    /// Must guarantee at least one connection in `self.connections`
        /// in case of success.
        pub async fn connect<'a>(bootstrap: &'a [&'a str]) -> io::Result<Self> {
            let connect_futures = bootstrap
                .iter()
                .filter_map(|addr| addr.parse().ok())
                .map(Broker::connect);




            // TODO: use `fold` to preserve and report last error if no result
            /*let resolved = futures::stream::futures_unordered(connectFutures).
                    filter_map(|f| future::ready(f.ok())).
                    next();
            let broker = match await!(resolved) {
                Some(broker) => broker,
                // TODO: failure
                None => return Err(io::Error::from(io::ErrorKind::NotFound)),
            };*/

            let x = futures::stream::poll_fn(connect_futures);

            let mut resolved1 = futures::stream::futures_unordered(connect_futures)
                .filter_map(|f| future::ready(f.ok()));
            let broker = match await!(resolved1.next()) {
                Some(broker) => broker,
                // TODO: failure
                None => return Err(io::Error::from(io::ErrorKind::NotFound)),
            };

            // TODO: move it to BrokerConnection
            debug!("Connected to {:?}", broker);

            /*let request = protocol::MetadataRequest0{topics};
            let response = await!(broker.request(&request));
            debug!("Metadata response: {:?}", response);
            */

            let bootstrap: Vec<String> = bootstrap.iter().map(|&a| a.to_string()).collect();
            /*Ok(Cluster {
                bootstrap,
                //topic_meta: response
            })*/
            Err(io::Error::from(io::ErrorKind::NotConnected))
        }
    */
}

fn start_topic_resolver(event_in: mpsc::UnboundedReceiver<String>, cluster_tx: mpsc::UnboundedSender<EventIn>, spawner: &mut LocalSpawner) -> mpsc::UnboundedReceiver<protocol::TopicMetadata> {
    let (response_tx, response_rx) = mpsc::unbounded();

    let timer = crate::timer::new(Duration::new(10, 0)).
        map(Either::Right);
    let event_in = event_in.map(Either::Left);
    let event_in = event_in.select(timer);

    let exec_loop = async move {
        let mut topic_list = HashSet::<String>::new();
        let mut topic_in_progress = HashSet::<String>::new();
        await!(event_in.for_each(|topic_or_timer| {
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
        }));
        debug!("topic_resolver loop exited");
    };

    spawner.spawn(exec_loop).unwrap();

    response_rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use simplelog::*;
    use futures::executor::LocalPool;

    #[test]
    fn resolve() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
        ])
        .unwrap();
        debug!("Starting test");

        let mut pool = LocalPool::new();
        let mut spawner = pool.spawner();
        pool.run_until(
            async {
                let addr = vec!["127.0.0.1:9092".to_string()];
                //let (tx, rx) = mpsc::unbounded();
                let mut channel = Cluster::new(addr, /*rx,*/ &mut spawner);
                //info!("Bootstrapped: {:?}", cluster);
                channel.tx.unbounded_send(EventIn::ResolveTopic("test1".to_string())).unwrap();
                let res = dbg!(await!(channel.rx.next()));
                //debug!("Got response");
            },
        );
    }
}
