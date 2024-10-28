use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use crate::cluster::LeaderMap;
use tokio::sync::mpsc;
use crate::types::{BrokerId, Partition, QueuedMessage};
use tracing::{debug, error, trace, warn};
use tracing_attributes::instrument;
use crate::connection::BrokerConnection;
use crate::connections_pool::{ConnectionPool, Entry};
use crate::meta_cache::MetaCache;
use crate::protocol;
use crate::protocol::TypedBuffer;

///
///
///                 | partition1 queue<messages>
///        | topic1-| partition2 queue<messages>
///        |        | partition3 queue<messages>
/// Buffer-|
///        |        | partition1 queue<messages>
///        | topic2-| partition2 queue<messages>
///
///
///
/// ProduceMessage --| broker_id 1 | topic 1 --|partition 0; recordset
///                  |                         |partition 1; recordset
///                  |
///                  | broker_id 2 | topic 1 --| partition 3; recordset
///                                |
///                                | topic 2 --| partition 0; recordset

pub struct Buffer {
    topic_queues: HashMap<String, Vec<PartitionQueue>>,
    meta_discover_tx: mpsc::Sender<String>,
    connection_request_tx: mpsc::Sender<BrokerId>,
    //connecting: FuturesUnordered<ConnectingFuture>,
    meta: MetaCache,
    // Current buffer size, in bytes
    size: usize,
    size_limit: usize,
}

#[derive(Debug, Default)]
struct PartitionQueue {
    pub(self) queue: VecDeque<QueuedMessage>,
    // How many message are being sent
    sending_count: u32,
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("topic_queues", &self.topic_queues.len())
            .field("size", &self.size)
            .field("size_limit", &self.size_limit)
            .finish()
    }
}

impl Buffer {
    pub fn new(meta_discover_tx: mpsc::Sender<String>, meta: MetaCache, connection_request_tx: mpsc::Sender<BrokerId>) -> Self {
        Buffer {
            topic_queues: HashMap::new(),
            meta_discover_tx,
            connection_request_tx,
            meta,
            size: 0,
            // TODO: make configurable
            size_limit: 100 * 1024 * 1024,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.topic_queues.iter().flat_map(|tq| tq.1).all(|pq| pq.queue.is_empty())
    }

    /// If buffer overflows, then message will be returned back.
    #[instrument(level="debug")]
    pub(crate) fn add(
        &mut self,
        msg: QueuedMessage,
        topic: &str,
        partition: Partition,
        partitions_count: u32)
     -> Option<QueuedMessage> {
        if self.size + msg.size() > self.size_limit {
            tracing::event!(tracing::Level::DEBUG, "Buffer overflow");
            //debug!("Overflow");
            return Some(msg);
        }

        let partitions = match self.topic_queues.get_mut(topic) {
            Some(partitions) => partitions,
            None => {
                let mut vec = Vec::with_capacity(partitions_count as usize);
                for i in 0..partitions_count as usize {
                    vec.push(PartitionQueue::default())
                }
                self.topic_queues.insert(topic.to_string(), vec);
                self.topic_queues.get_mut(topic).unwrap()
            }
        };

        self.size += msg.size();
        partitions[partition as usize].queue.push_back(msg);
        debug!("Added message to partition {partition}");

        None
    }

    /// Collect messages into Produce Requests and pair them with connections from Connections Pool
    #[instrument(level = "debug", skip(self, connections))]
    pub(crate) fn flush_request(&mut self, connections: &mut ConnectionPool) -> Vec<(TypedBuffer<protocol::ProduceResponse3>, Box<BrokerConnection>)> {
        debug!("flush_request");
        let leaders = self.meta.get_known_broker_map();
        let requests = self.group_queue_by_leader(&leaders);

        trace!("Bytes: {}", self.size);
        trace!("Requests: {:?}", requests);
        trace!("MetaCache: {:?}", self.meta);

        // send only entries which have connection available
        let connected: Vec<_> = requests.into_iter()
            .filter_map(|(broker_id, request_topics)| {
                trace!("Looking for connection for broker_id: {broker_id}");
                let conn = connections.get(&broker_id);
                match conn {
                    // Connection found, take it
                    Entry::Available(conn) => {
                        debug!("Got active connection from pool");
                        Some((broker_id, request_topics, conn))
                    }
                    Entry::Lent => {
                        debug!("Connection is busy in the pool");
                        None
                    }
                    Entry::Connecting => {
                        debug!("Connection is connecting in the pool");
                        None
                    }
                }
            }).map(|(broker_id, topics, mut conn)| {
                let request = protocol::ProduceRequest3 {
                    transactional_id: None,
                    acks: 1,    // TODO: config
                    timeout: 1500,  // TODO: config
                    topic_data: &topics,
                };
                let buffer = conn.write_request(&request);
                (TypedBuffer::new(buffer), conn)
            }).collect();

        connected
    }

    /// Scan message buffer and group messages by the leader and update `sending` counter.
    /// Two queues because that's how dequeue works.
    /// Returns leader->topic->partition->messages plus info about how many messages are being sent per partition queue
    /// so the queue can be truncated upon successful send.
    pub(crate) fn group_queue_by_leader<'a>(&'a self, leader_map: &'a LeaderMap/*&'a[(BrokerId, Vec<(String, Vec<Partition>)>)]*/) ->
        HashMap<BrokerId, HashMap<&'a str, HashMap<Partition, (&'a [QueuedMessage], &'a [QueuedMessage])>>>
    {
        let mut requests = HashMap::new();

        for (leader, topics) in leader_map {

            let mut topics_data: HashMap<&str, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>> = HashMap::new();
            let mut length = HashMap::new();
            for (topic, partitions) in topics {

                let mut topic_data: HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])> = HashMap::new();
                let pqs = match self.topic_queues.get(topic) {
                    None => continue,
                    Some(pqs) => pqs,
                };

                for partition in partitions {
                    let pq = match pqs.get(*partition as usize) {
                        Some(pq) => pq,
                        None => {
                            error!("Number of partitions in message queue does not match. Queue size: {} missing partition: {} ", pqs.len(), partition);
                            continue;
                        }
                    };

                    // TODO: limit message size
                    let len = pq.queue.len();
                    topic_data.insert(*partition, pq.queue.as_slices());
                    length.insert(topic.to_string(), (partition, len));

                    // TODO: limit message size
                    // pq.sending = pq.queue.len().try_into().unwrap();
                    // sending.push((pq, pq.queue.len()));

                    // broker_partitioned.entry(leader).or_default()
                    //     .entry(topic).or_default()
                    //     .insert(partition, pq.queue.as_slices());
                }

                topics_data.insert(topic, topic_data);
            }

            requests.insert(*leader, topics_data);
        }


        requests
    }

    #[instrument(level = "debug")]
    pub(crate) fn discard(&mut self, partitions: &protocol::ProduceResponse3 /*&[(&str,&[Partition])]*/) {
        debug!("discard()");
        for response in &partitions.responses {
            if let Some(mut queues) = self.topic_queues.get_mut(&response.topic) {
                for partition in &response.partition_responses {
                    if !partition.error_code.is_ok() {
                        continue;
                    }
                    if (partition.partition as usize) < queues.len() {
                        let queue = &mut queues[partition.partition as usize];
                        let trim_count = queue.sending_count as usize;
                        queue.queue.drain(..trim_count);

                    } else {
                        warn!(name: "Discard unknown", topic = response.topic, partition = %partition.partition);
                    }
                }
            } else {
                warn!(name: "Discard unknown", topic = %response.topic);
            }
        }
    }
}


