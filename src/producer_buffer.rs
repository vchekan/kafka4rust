use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use crate::cluster::LeaderMap;
use bytes::BytesMut;
use tokio::sync::mpsc;
use crate::types::{BrokerId, Partition, QueuedMessage};
use tracing::{debug, error, trace};
use tracing_attributes::instrument;
use crate::connection::BrokerConnection;
use crate::connections_pool::{ConnectionPool, Entry};
use crate::meta_cache::MetaCache;
use crate::protocol;

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
    queue: VecDeque<QueuedMessage>,
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
        // let partitions = self.topic_queues.entry(topic).or_insert_with(|| {
        //     vec![PartitionQueue::default(); partitions_count as usize]
        // });
        self.size += msg.size();
        partitions[partition as usize].queue.push_back(msg);
        debug!("Added message to partition {partition}");

        None
    }

    /// Collect messages into Produce Requests and pair them with connections from Connections Pool
    #[instrument(level = "debug", skip(self, connections))]
    pub(crate) fn flush_request<'a, 'b>(&'a mut self, connections: &'b mut ConnectionPool) -> Vec<(BytesMut, Box<BrokerConnection>)> {
        debug!("flush_request");
        let leaders = self.meta.get_known_broker_map();
        let requests = self.group_queue_by_leader(&leaders);

        trace!("Size: {}", self.size);
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
                        trace!("Got active connection from pool");
                        Some((broker_id, request_topics, conn))
                    }
                    Entry::Lent => {
                        trace!("Connection is busy in the pool");
                        None
                    }
                    Entry::Connecting => {
                        trace!("Connection is connecting in the pool");
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
                // let mut buffer = BytesMut::with_capacity(1024*1024);
                // request.serialize(&mut buffer);
                (buffer, conn)
            }).collect();

        connected
    }

    fn scan_partitions_to_send(&self) -> Vec<(&str, Vec<Partition>)> {
        self.topic_queues.iter()
            .map(|(topic, parts)| {
                (topic.as_str(), (0..parts.len()).map(|l| l as Partition).collect())
            }).collect()
    }

    // /// TODO: rust BC to become smarter. Parameter `cluster` is member of `self` but I have to pass it separately because borrow checker
    // /// complains about `self` being borrowed 2 times mutably.
    // #[instrument(level = "debug", err, skip(self, acks))]
    // pub async fn flush(&mut self, acks: &mpsc::Sender<Response>/*, cluster: &ClusterHandler*/) -> BrokerResult<()> {
    //     // Have to clone to break immutable and mutable (self.group_queue_by_leader) reference
    //     let topics: Vec<_> = self.topic_queues.keys().cloned().collect();
    //     // TODO: timeout from settings
    //     let leaders = cluster.resolve(topics).await;
    //     let broker_partitioned = self.group_queue_by_leader(&leaders);
    //
    //     // Need to collect partition success or failure to adjust buffer,
    //     // and re-issue fetching topic metadata.
    //     // If contacting broker failed, then re-fetch meta for every topic in this broker.
    //     let mut responses: Vec<(BrokerId, Option<ProduceResponse3>)> = vec![];
    //
    //     // TODO: send to all leaders in parallel. Use `Stream`?
    //     for (leader, /*(data, lengths)*/request) in broker_partitioned {
    //         // let res: Vec<_> = broker_partitioned.into_iter().map(|(leader, data)| tokio::spawn(async move {
    //         // let responses = async_stream::stream! {
    //         // for (&leader, data) in &broker_partitioned {
    //         // TODO: stream requires `static lifetime but we have messages as references inside buffer...
    //         //let mut responses = stream::iter(broker_partitioned).map(|(leader, data)| async move {
    //         // let request = protocol::ProduceRequest3 {
    //         //     transactional_id: None,
    //         //     acks: 1,
    //         //     timeout: 1500,
    //         //     topic_data: todo!()//data,
    //         // };                                                                                                   F
    //
    //
    //         // TODO: upon failure to connect to broker, reset connection and refresh metadata
    //         let conn = match cluster.broker_by_id(leader).await {
    //             Some(broker) => broker,
    //             None => /*return*/ {
    //                 // cluster.start_resolving_topics(data.keys().map(|t| *t)).await?;
    //                 //yield (leader, Err(e));
    //                 // return Err(e);
    //                 // self.mark_leader_down(leader);
    //                 responses.push((leader, None));
    //                 continue;
    //             }
    //         };
    //
    //         // let cmd_buf = broker.mk_request(request);
    //         debug!("Sending Produce");
    //         let request = protocol::ProduceRequest3 {
    //             transactional_id: None,
    //             // TODO: config
    //             acks: 1,
    //             // TODO: config
    //             timeout: 1500,
    //             topic_data: &request,
    //         };
    //         let response = conn.exchange(&request).await;
    //         // TODO: throttle_time
    //
    //         let response = match response {
    //             Err(e) => {
    //                 // Send was not successful.
    //                 // Reset broker. This will cause fresh metadata fetch in the next round
    //                 // cluster.reset_broker(leader);
    //                 // TODO: `continue` with other partitions instead of `return`
    //                 /*return*/
    //                 // yield (leader, Err(e));
    //                 // return Err(e);
    //                 // self.mark_leader_down(leader);
    //                 responses.push((leader, None));
    //                 continue;
    //             }
    //             Ok(response) => response
    //         };
    //
    //         // Container to keep successful topic/partitions after the call loop. We can not modify queue
    //         // in the loop because loop keeps immutable reference to `self`.
    //         // Topic -> partition
    //         // let mut messages_to_discard = HashMap::<String, Vec<u32>>::new();
    //
    //         debug!("Got Produce response: {:?}", response);
    //         for topic_resp in &response.responses {
    //             for partition_resp in &topic_resp.partition_responses {
    //                 let ack = if partition_resp.error_code.is_ok() {
    //                     debug!("Ok sent partition {} to broker {:?}", partition_resp.partition, conn);
    //
    //                     // messages_to_discard
    //                     //     .entry(topic_resp.topic.clone())
    //                     //     .or_default()
    //                     //     .push(partition_resp.partition as u32);
    //
    //                     Response::Ack {
    //                         partition: partition_resp.partition as u32,
    //                         offset: partition_resp.base_offset as u64,
    //                         error: partition_resp.error_code,
    //                     }
    //                 } else {
    //                     error!(
    //                         "Produce error. Topic {} partition {} error {:?}",
    //                         topic_resp.topic, partition_resp.partition, partition_resp.error_code
    //                     );
    //                     tracing::event!(tracing::Level::ERROR, ?leader, ?topic_resp.topic, partition = ?partition_resp.partition, error_code = ?partition_resp.error_code, "Produce error");
    //                     Response::Nack {
    //                         partition: partition_resp.partition as u32,
    //                         error: partition_resp.error_code,
    //                     }
    //                 };
    //
    //                 // TODO: send ack
    //             }
    //         }
    //         responses.push((leader, Some(response)));
    //     }
    //
    //     // `broker_partitioned` keeps `&mut self` reference which makes it impossible to mutate `self.topic_queues`,
    //     // so shadow it with only parts we need
    //     // TODO: this feels clumsy. When no error happen, it cause redundant `clone()`. Think better borrow design.
    //     // let broker_partitioned: HashMap<BrokerId,Vec<&str>> = broker_partitioned.into_iter()
    //         // .map(|e| (e.0, e.1.keys().into_iter().map(|k| (*k).clone()).collect())).collect();
    //         // .map(|(leader, topics)| (leader, topics.keys().map(|t| *t).collect::<Vec<_>>())).collect();
    //
    //
    //     // for(pq, len) in sending {
    //     //     pq.queue.drain(..len);
    //     // }
    //
    //
    //     //
    //     // Discard messages which were sent successfully.
    //     //
    //     for (leader, response) in responses {
    //         match response {
    //             Some(response) => {
    //                 for response in response.responses {
    //                     let topic = response.topic;
    //                     let queue_partitions = match self.topic_queues.get_mut(&topic) {
    //                         Some(p) => p,
    //                         None => {
    //                             error!("Can not find topic for produce response: {}", topic);
    //                             continue;
    //                         }
    //                     };
    //                     for response in response.partition_responses {
    //
    //
    //                         todo!()
    //                         // let queues = self.topic_queues.get_mut(&topic).expect("Topic not found");
    //                         // let partition = response.partition;
    //                         // //for partition in response.responses {
    //                         // let queue = &mut queues[partition as usize];
    //                         // queue.queue.drain(..queue.sending as usize);
    //                         // queue.sending = 0;
    //                         // let remaining = queue.queue.len();
    //                         // event!(
    //                         //         Level::DEBUG,
    //                         //         ?topic,
    //                         //         ?partition,
    //                         //         ?remaining,
    //                         //         "Truncated queue");
    //                     }
    //                 }
    //             }
    //             // leader failed and topology of all topics served by this leader need to be refreshed
    //             None => {
    //                 // let topics = broker_partitioned.get(&leader)
    //                 //     Leader is put into `responses` from `broker_partitioned`, so no chance for it to be missing
    //                     // .unwrap();
    //
    //                 // match broker_partitioned.get(&leader) {
    //                 //     Some((_, lens)) => {
    //                 //         self.cluster.mark_leader_down(leader).await;
    //                 //         info!("Marked leader down: {}", leader);
    //                 //         // TODO: at least `warn` if error sending to pipeline
    //                 //         self.cluster.start_resolving_topics(lens.keys()).await;
    //                 //         // for topic in lens.keys() {
    //                 //             // TODO: at least `warn` if error sending to pipeline
    //                 //             // let _ = self.cluster.start_resolving_topics(&[topic]).await;
    //                 //         // }
    //                 //
    //                 //     }
    //                 //     None => warn!("Response contains broker which was not requested: {}", leader)
    //                 // }
    //             }
    //         }
    //     }
    //
    //     Ok(())
    // }

    /// Scan message buffer and group messages by the leader and update `sending` counter.
    /// Two queues because that's how dequeue works.
    /// Returns leader->topic->partition->messages plus info about how many messages are being sent per partition queue
    /// so the queue can be truncated upon successful send.
    pub(crate) fn group_queue_by_leader<'a>(&'a self, leader_map: &'a LeaderMap/*&'a[(BrokerId, Vec<(String, Vec<Partition>)>)]*/) ->
        // TODO: think how to integrate response with in-flight queue length
        // (HashMap<BrokerId, (BytesMut, HashMap<String,(Partition,usize)>)>)
        HashMap<BrokerId, HashMap<&'a str, HashMap<Partition, (&'a [QueuedMessage], &'a [QueuedMessage])>>>
    {
        // TODO: implement FIFO and request size bound algorithm

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

            // let request = protocol::ProduceRequest3 {
            //     transactional_id: None,
            //     // TODO: config
            //     acks: 1,
            //     // TODO: config
            //     timeout: 1500,
            //     topic_data: &topics_data,
            // };

            // let mut buffer = buffer_pool::get();
            // TODO: get client_id from settings
            // protocol::write_request(&request, None, &mut buffer);
            requests.insert(*leader, topics_data);
        }


        requests
        //broker_partitioned
    }
}

mod test {

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        // let cluster = ClusterHandler::with_bootstrap("localhost", None)?;
        // let mut buff = Buffer::new(cluster);

        // buff.add("").await;

        Ok(())
    }

}

