use crate::bindings::*;
use std::os::raw::{c_char, c_int, c_void};
use lazy_static::lazy_static;
use std::sync::Mutex;
use crate::dissects::*;
use crate::utils::i8_str;
use crate::fields::*;

//
// RecordBatch
//
/*protocol!(RecordBatch => {
    base_offset: i64,
    batch_length: i32
    partitionLeaderEpoch: i32,
    magic: i8 = 2,
    crc: i32,
    attributes: i16, // TODO: bitset
    last_offset_delta: i32,
    first_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i64,
    base_sequence: i32,
    records: []
});
*/


//
// Api Key 0: Produce
//
protocol!(ProduceRequest => {
    acks: {hf_kafka_acks: i16},
    timeout: {hf_kafka_timeout: i32},
    topic_data: [TopicData ETT_TOPIC_DATA]
});

protocol!(TopicData => {
    topic: {hf_kafka_topic_name: String, ETT_PRODUCE_REQUEST_TOPIC},
    partition: {hf_kafka_partition: i32}
    //record_set: RecordBatch
});

//
//
//
protocol!(metadata_response => {
    throttle_time_ms/3 : {hf_kafka_throttle_time_ms: i32},
    Brokers: [Broker ETT_TOPICS],
    cluster_id/2 : {hf_kafka_cluster_id: String, ETT_CLUSTER_ID},
    controller_id/1 : {hf_kafka_controller_id: i32},
    topic_metadata : [TopicMetadata ETT_METADATA_TOPIC]
});

protocol!(TopicMetadata => {
    error_code : {hf_kafka_error: i16},
    topic : {hf_kafka_topic_name: String, ETT_METADATA_TOPIC},
    is_internal/1 : {hf_kafka_is_internal: bool },
    partition_metadata : [PartitionMetadata ETT_PARTITION_METADATA]
});

protocol!(PartitionMetadata => {
    error_code : {hf_kafka_error: i16},
    partition : {hf_kafka_partition: i32},
    leader : {hf_kafka_metadata_leader: i32},
    leader_epoch/7 : {hf_kafka_metadata_leader_epoch: i32},
    replicas: [Replica ETT_REPLICAS],
    isr: [Isr ETT_ISR],
    offline_replicas/5 : [OfflineReplicas ETT_OFLINE_REPLICAS]
});

protocol!(Broker => {
    node_id: {hf_kafka_node_id: i32},
    host: {hf_kafka_host: String, ett_broker_host},
    port: {hf_kafka_port: i32},
    rack/1 : {hf_kafka_rack: String, ETT_RACK}
});

protocol!(Replica => {
    replica: {hf_kafka_metadata_replicas: i32}
});

protocol!(Isr => {
    isr: {hf_kafka_metadata_isr: i32}
});

protocol!(OfflineReplicas => {
    offline_replica: {hf_kafka_offline_replica: i32}
});
