use std::collections::{HashMap, VecDeque};
use crate::{ClusterHandler, protocol, buffer_pool};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::protocol::{ProduceResponse3};
use crate::error::BrokerResult;
use std::time::Duration;
use crate::retry_policy::with_retry;
use crate::types::{BrokerId, Partition, QueuedMessage};
use tracing::{self, event, Level};
use tokio::sync::RwLock;
use bytes::BytesMut;
use log::{debug, info, warn, error};
use crate::producer::Response;

/// Q: should buffer data be shared or copied when sending to broker?
/// A:
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
///                  | broker_id 2 | topic 2 --| partition 0; recordset
///                                |
///                                | topic 3 --| partition 0; recordset
///
#[derive(Debug)]
pub(crate) struct Buffer {
    topic_queues: HashMap<String, Vec<PartitionQueue>>,
    // Current buffer size, in bytes
    size: u32,
    size_limit: u32,
    cluster: ClusterHandler,
}

impl Buffer {
    pub fn new(cluster: ClusterHandler) -> Self {
        Buffer {
            topic_queues: HashMap::new(),
            size: 0,
            // TODO: make configurable
            size_limit: 100 * 1024 * 1024,
            // meta_cache: HashMap::new(),
            cluster,
        }
    }

    /// Is async because topic metadata might require resolving.
    /// At the same time, we do not want to blow memory with awaiting tasks
    /// if resolving takes time and message velocity is high.
    pub async fn add(
        &mut self,
        msg: QueuedMessage,
        topic: String,
        partition: u32,
        partitions_count: u32,
    ) -> BufferingResult {
        if self.size + msg.value.len() as u32 > self.size_limit {
            debug!("Overflow");
            // TODO: await instead
            return BufferingResult::Overflow;
        }

        let mut topic_queues = self.topic_queues.write().await;
        let partitions = topic_queues.entry(topic).or_insert_with(|| {
            (0..partitions_count)
                .map(|_| PartitionQueue::default())
                .collect()
        });

        self.size += (msg.value.len() + msg.key.as_ref().map(|k| k.len()).unwrap_or(0)) as u32;

        partitions[partition as usize].queue.push_back(msg);
        BufferingResult::Ok
    }

    /// TODO: rust BC to become smarter. Parameter `cluster` is member of `self` but I have to pass it separately because borrow checker
    /// complains about `self` being borrowed 2 times mutably.
    //#[instrument(level = "debug", err, skip(self, acks, cluster))]
    pub async fn flush(&self, acks: &Sender<Response>, cluster: &ClusterHandler) -> BrokerResult<()> {

        // let mut topics_meta = vec![];
        // for topic in self.topic_queues.keys() {
        //     let meta = cluster.get_or_request_meta(topic)
        //         .with_retry(Duration::from_secs(1))
        //         // TODO .timeout()
        //         .await;
        //     topics_meta.push(meta);
        // }

        // Have to clone to break immutable and mutable (self.group_queue_by_leader) reference
        let topics: Vec<_> = self.topic_queues.keys().cloned().collect();
        // TODO: timeout from settings
        let leaders = with_retry(Duration::from_secs(1), Duration::from_secs(60),
                                 || { cluster.get_or_request_leader_map(topics.as_slice()) }).await?;
        let mut broker_partitioned = self.group_queue_by_leader(leaders).await;

        // Need to collect partition success or failure to adjust buffer,
        // and re-issue fetching topic metadata.
        // If contacting broker failed, then re-fetch meta for every topic in this broker.
        let mut responses: Vec<(BrokerId, BrokerResult<ProduceResponse3>)> = vec![];

        // TODO: send to all leaders in parallel. Use `Stream`?
        for (&leader, (data, lengths)) in &mut broker_partitioned {
            // let res: Vec<_> = broker_partitioned.into_iter().map(|(leader, data)| tokio::spawn(async move {
            // let responses = async_stream::stream! {
            // for (&leader, data) in &broker_partitioned {
            // TODO: stream requires `static lifetime but we have messages as references inside buffer...
            //let mut responses = stream::iter(broker_partitioned).map(|(leader, data)| async move {
            // let request = protocol::ProduceRequest3 {
            //     transactional_id: None,
            //     acks: 1,
            //     timeout: 1500,
            //     topic_data: todo!()//data,
            // };


            // TODO: upon failure to connect to broker, reset connection and refresh metadata
            let broker = match cluster.broker_get_no_connect(leader).await {
                Ok(broker) => broker,
                Err(e) => /*return*/ {
                    // cluster.start_resolving_topics(data.keys().map(|t| *t)).await?;
                    //yield (leader, Err(e));
                    // return Err(e);
                    // self.mark_leader_down(leader);
                    responses.push((leader, Err(e)));
                    continue;
                }
            };

            // let cmd_buf = broker.mk_request(request);
            debug!("Sending Produce");
            // Any failure to
            let response = broker
                .send_request2::<protocol::ProduceResponse3>(data)
                .await;
            // TODO: throttle_time

            let response = match response {
                Err(e) => {
                    // Send was not successful.
                    // Reset broker. This will cause fresh metadata fetch in the next round
                    // cluster.reset_broker(leader);
                    // TODO: `continue` with other partitions instead of `return`
                    /*return*/
                    // yield (leader, Err(e));
                    // return Err(e);
                    // self.mark_leader_down(leader);
                    responses.push((leader, Err(e)));
                    continue;
                }
                Ok(response) => response
            };

            // Container to keep successful topic/partitions after the call loop. We can not modify queue
            // in the loop because loop keeps immutable reference to `self`.
            // Topic -> partition
            // let mut messages_to_discard = HashMap::<String, Vec<u32>>::new();

            debug!("Got Produce response: {:?}", response);
            for topic_resp in &response.responses {
                for partition_resp in &topic_resp.partition_responses {
                    let ack = if partition_resp.error_code.is_ok() {
                        debug!("Ok sent partition {} to broker {:?}", partition_resp.partition, broker);

                        // messages_to_discard
                        //     .entry(topic_resp.topic.clone())
                        //     .or_default()
                        //     .push(partition_resp.partition as u32);

                        Response::Ack {
                            partition: partition_resp.partition as u32,
                            offset: partition_resp.base_offset as u64,
                            error: partition_resp.error_code,
                        }
                    } else {
                        error!(
                            "Produce error. Topic {} partition {} error {:?}",
                            topic_resp.topic, partition_resp.partition, partition_resp.error_code
                        );
                        tracing::event!(tracing::Level::ERROR, ?leader, ?topic_resp.topic, partition = ?partition_resp.partition, error_code = ?partition_resp.error_code, "Produce error");
                        Response::Nack {
                            partition: partition_resp.partition as u32,
                            error: partition_resp.error_code,
                        }
                    };

                    // TODO: send ack
                }
            }
            responses.push((leader, Ok(response)));
        }

        // `broker_partitioned` keeps `&mut self` reference which makes it impossible to mutate `self.topic_queues`,
        // so shadow it with only parts we need
        // TODO: this feels clumsy. When no error happen, it cause redundant `clone()`. Think better borrow design.
        // let broker_partitioned: HashMap<BrokerId,Vec<&str>> = broker_partitioned.into_iter()
            // .map(|e| (e.0, e.1.keys().into_iter().map(|k| (*k).clone()).collect())).collect();
            // .map(|(leader, topics)| (leader, topics.keys().map(|t| *t).collect::<Vec<_>>())).collect();


        // for(pq, len) in sending {
        //     pq.queue.drain(..len);
        // }


        // Discard messages which were sent successfully.
        // Get Write lock to trim queues
        let mut topic_queues = self.topic_queues.write().await;
        for (leader, response) in responses {
            match response {
                Ok(response) => {
                    for response in response.responses {
                        let topic = response.topic;
                        let queue_partitions = match topic_queues.get_mut(&topic) {
                            Some(p) => p,
                            None => {
                                error!("Can not find topic for produce response: {}", topic);
                                continue;
                            }
                        };
                        for response in response.partition_responses {


                            todo!()
                            // let queues = self.topic_queues.get_mut(&topic).expect("Topic not found");
                            // let partition = response.partition;
                            // //for partition in response.responses {
                            // let queue = &mut queues[partition as usize];
                            // queue.queue.drain(..queue.sending as usize);
                            // queue.sending = 0;
                            // let remaining = queue.queue.len();
                            // event!(
                            //         Level::DEBUG,
                            //         ?topic,
                            //         ?partition,
                            //         ?remaining,
                            //         "Truncated queue");
                        }
                    }
                }
                // leader failed and topology of all topics served by this leader need to be refreshed
                Err(e) => {
                    // let topics = broker_partitioned.get(&leader)
                    //     Leader is put into `responses` from `broker_partitioned`, so no chance for it to be missing
                        // .unwrap();

                    match broker_partitioned.get(&leader) {
                        Some((_, lens)) => {
                            self.cluster.mark_leader_down(leader).await;
                            info!("Marked leader down: {}", leader);
                            // TODO: at least `warn` if error sending to pipeline
                            self.cluster.start_resolving_topics(lens.keys()).await;
                            // for topic in lens.keys() {
                                // TODO: at least `warn` if error sending to pipeline
                                // let _ = self.cluster.start_resolving_topics(&[topic]).await;
                            // }

                        }
                        None => warn!("Response contains broker which was not requested: {}", leader)
                    }
                }
            }
        }

        Ok(())
    }

    /// Scan message buffer and group messages by the leader and update `sending` counter.
    /// Two queues because that's how dequeue works.
    /// Returns leader->topic->partition->messages plus info about how many messages are being sent per partition queue
    /// so the queue can be truncated upon successful send.
    async fn group_queue_by_leader(&self, leader_map: Vec<(BrokerId, Vec<(&str, Vec<Partition>)>)>) ->
        // TODO: think how to integrate response with in-flight queue length
        (HashMap<BrokerId, (BytesMut, HashMap<String,(Partition,usize)>)>)
        //HashMap<BrokerId, HashMap<&'s str, HashMap<Partition, (&'s [QueuedMessage], &'s [QueuedMessage])>>>
    {
        // TODO: implement FIFO and request size bound algorithm

        // leader->topic->partition->recordset[]
        // let mut broker_partitioned = HashMap::<
        //     BrokerId,
        //     HashMap<&str, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>>
        // >::new();

        let mut requests = HashMap::new();

        let topic_queues = self.topic_queues.read().await;
        for (leader, topics) in leader_map {
            let mut topics_data: HashMap<&str, HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])>> = HashMap::new();
            let mut length = HashMap::new();
            for (topic, partitions) in topics {

                let mut topic_data: HashMap<Partition, (&[QueuedMessage], &[QueuedMessage])> = HashMap::new();
                let pqs = match topic_queues.get(topic) {
                    None => continue,
                    Some(pqs) => pqs,
                };

                for partition in partitions {
                    let pq = match pqs.get(partition as usize) {
                        Some(pq) => pq,
                        None => {
                            error!("Number of partitions in message queue does not match. Queue size: {} missing partition: {} ", pqs.len(), partition);
                            continue;
                        }
                    };

                    // TODO: limit message size
                    let len = pq.queue.len();
                    topic_data.insert(partition, pq.queue.as_slices());
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

            let request = protocol::ProduceRequest3 {
                transactional_id: None,
                // TODO: config
                acks: 1,
                // TODO: config
                timeout: 1500,
                topic_data: &topics_data,
            };

            let mut buffer = buffer_pool::get();
            // TODO: get client_id from settings
            protocol::write_request(&request, None, &mut buffer);
            requests.insert(leader, (buffer, length));
        }


        requests
        //broker_partitioned
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum BufferingResult {
    Ok,
    Overflow,
}

#[derive(Debug, Default)]
struct PartitionQueue {
    queue: VecDeque<QueuedMessage>,
    // How many message are being sent
    sending: u32,
}
