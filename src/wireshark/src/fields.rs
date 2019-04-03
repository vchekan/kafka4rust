use crate::bindings::*;
use crate::utils::i8_str;
use lazy_static::lazy_static;
use std::sync::Mutex;
use std::os::raw::{c_char, c_int, c_void};

ett!(ETT_KAFKA, ett_broker_host, ETT_TOPICS, ETT_CLIENT_ID,
    ETT_METADATA_REQ_TOPICS,
    ETT_SUPPORTED_API_VERSIONS,
    ETT_RESPONSE_METADATA_BROKERS,
    ETT_METADATA_BROKER,
    ETT_METADATA_TOPIC,
    ETT_CLUSTER_ID,
    ETT_RACK,
    ETT_TOPIC_METADATA_TOPICS,
    ETT_PARTITION_METADATA,
    ETT_REPLICAS,
    ETT_ISR,
    ETT_OFLINE_REPLICAS,
    ETT_TOPIC_DATA,
    ETT_TOPIC_DATA2,
    ETT_PRODUCE_REQUEST_TOPIC,
    ETT_BATCH_ATTRIBUTES
);

header_fields!(
    {hf_kafka_node_id, "Node Id\0", "kafka.broker.node\0", ftenum_FT_INT32, "Broker node Id.\0"},
    {hf_kafka_host, "Host\0", "kafka.broker.host\0", "Broker host name.\0"},
    {hf_kafka_port, "Port\0", "kafka.broker.port\0", ftenum_FT_INT32, "Broker port.\0"},
    {hf_kafka_len, "Length\0", "kafka.len\0", ftenum_FT_INT32, "The length of this Kafka packet.\0"},
    {hf_kafka_request_api_key, "API Key\0", "kafka.request_key\0", ftenum_FT_INT16, "Request API.\0", kafka_api_names},
    {hf_kafka_correlation_id, "Correlation Id\0", "kafka.correlation_id\0", ftenum_FT_INT32, "Correlation Id.\0"},
    {hf_kafka_request_api_version, "API Version\0", "kafka.request.version\0", ftenum_FT_INT16, "Request API Version.\0"},
    {hf_kafka_string_len, "String Length\0", "kafka.string_len\0", ftenum_FT_INT16, "Generic length for kafka-encoded string.\0"},
    {hf_kafka_string, "String\0", "kafka.string\0", "String primitive value.\0"},
    {hf_kafka_client_id, "Client Id\0", "kafka.client_id\0", "The ID of the sending client.\0"},
    {hf_kafka_array_count, "Array Count\0", "kafka.array_count\0", ftenum_FT_INT32, "Array count\0"},
    {hf_kafka_controller_id, "Controller Id\0", "kafka.topic_metadata.controller_id\0", ftenum_FT_INT32, "Topic metadata controller Id.\0"},
    {hf_kafka_cluster_id, "Cluster Id\0", "kafka.topic_metadata.cluster_id\0", "Topic metadata cluster Id.\0"},
    {hf_kafka_error, "Error\0", "kafka.error\0", ftenum_FT_INT16, "Kafka broker error.\0"},
    {hf_kafka_topic_name, "Topic Name\0", "kafka.topic_name\0", "Topic name.\0"},
    {hf_kafka_is_internal, "Is internal\0", "kafka.is_internal\0", ftenum_FT_BOOLEAN, "Is internal topic.\0"},
    {hf_kafka_partition, "Partition\0", "kafka.partition\0", ftenum_FT_INT32, "Topic metadata partition.\0"},
    {hf_kafka_metadata_leader, "Leader\0", "kafka.topic_metadata.leader\0", ftenum_FT_INT32, "Topic metadata leader.\0"},
    {hf_kafka_metadata_replicas, "Replicas\0", "kafka.topic_metadata.replicas\0", ftenum_FT_INT32, "Topic metadata replicas.\0"},
    {hf_kafka_metadata_isr, "Isr\0", "kafka.topic_metadata.isr\0", ftenum_FT_INT32, "Topic metadata isr.\0"},
    {hf_kafka_rack, "Rack\0", "kafka.broker.rack\0", "Broker rack.\0"},
    {hf_kafka_throttle_time_ms, "Throttle time\0", "kafka.throttle_time\0", ftenum_FT_INT32, "Response throttle time in ms.\0"},
    {hf_kafka_offline_replica, "Offline replicas\0", "kafka.offline_replica\0", ftenum_FT_INT32, "Offline replicas\0"},
    {hf_kafka_metadata_leader_epoch, "Leader epoch\0", "kafka.topic_metadata.leader_epoch\0", ftenum_FT_INT32, "Topic metadata leader epoch.\0"},
    {hf_kafka_acks, "Acks\0", "kafka.acks\0", ftenum_FT_INT16, "Acks requested\0"},
    {hf_kafka_timeout, "Timeout\0", "kafka.timeout\0", ftenum_FT_INT32, "Timeout (ms)\0"},
    // Record Batch
    {hf_kafka_recordbatch_segment_size, "Segment size\0", "kafka.recordbatch.segment_size\0", ftenum_FT_INT32, "Record batch segment size (bytes)\0"},
    {hf_kafka_recordbatch_baseoffset, "Base offset\0", "kafka.recordbatch.baseoffset\0", ftenum_FT_INT64, "Record batch base offset\0"},
    {hf_kafka_recordbatch_batchlength, "Batch length\0", "kafka.recordbatch.batchlength\0", ftenum_FT_INT32, "Record batch length\0"},
    {hf_kafka_recordbatch_partition_leader_epoch, "Partition leader epoch\0", "kafka.recordbatch.partitionleaderepoch\0", ftenum_FT_INT32, "Record batch partition leader epoch\0"},
    {hf_kafka_recordbatch_magic, "Magic\0", "kafka.recordbatch.magic\0", ftenum_FT_UINT8, "Record batch magic\0"},
    {hf_kafka_recordbatch_crc, "Crc\0", "kafka.recordbatch.crc\0", ftenum_FT_UINT32|field_display_e_BASE_HEX, "Record batch CRC\0"},
    {hf_kafka_recordbatch_attributes, "Attributes\0", "kafka.recordbatch.attributes\0", ftenum_FT_UINT16, "Record batch attributes\0"},
    {hf_kafka_recordbatch_lastoffsetdelta, "Last offset delta\0", "kafka.recordbatch.lastoffsetdelta\0", ftenum_FT_INT32, "Record batch last offset delta\0"},
    {hf_kafka_recordbatch_firsttimestamp, "First timestamp\0", "kafka.recordbatch.firsttimestamp\0", ftenum_FT_INT64, "Record batch first timestamp\0"},
    {hf_kafka_recordbatch_maxtimestamp, "Max timestamp\0", "kafka.recordbatch.maxtimestamp\0", ftenum_FT_INT64, "Record batch max timestamp\0"},
    {hf_kafka_recordbatch_producer_id, "Producer Id\0", "kafka.recordbatch.producerid\0", ftenum_FT_INT64, "Record batch producer id\0"},
    {hf_kafka_recordbatch_producer_epoch, "Producer epoch\0", "kafka.recordbatch.producer_epoch\0", ftenum_FT_INT16, "Record batch producer epoch\0"},
    {hf_kafka_recordbatch_base_sequence, "Base sequence\0", "kafka.recordbatch.base_sequence\0", ftenum_FT_INT32, "Record batch base sequence\0"},
    {hf_kafka_recordbatch_key, "Key\0", "kafka.record.key\0", ftenum_FT_BYTES|field_display_e_BASE_NONE, "Record key\0"},
    {hf_kafka_recordbatch_value, "Value\0", "kafka.record.value\0", ftenum_FT_BYTES|field_display_e_SEP_SPACE, "Record value\0"},
    // MessageSet
    {hf_kafka_messageset_offset, "Offset\0", "kafka.messageset.offset\0", ftenum_FT_INT64, "Message set offset\0"},
    {hf_kafka_messageset_msg_size, "Message size\0", "kafka.messageset.message_size\0", ftenum_FT_INT32, "Message set message size\0"},
    {hf_kafka_messageset_attributes, "Attributes\0", "kafka.messageset.attributes\0", ftenum_FT_UINT8, "Record batch attributes\0"},
    {hf_kafka_recordbatch_timestamp, "Timestamp\0", "kafka.messageset.timestamp\0", ftenum_FT_ABSOLUTE_TIME|absolute_time_display_e_ABSOLUTE_TIME_UTC, "Message set timestamp\0"}
);

pub(crate) static mut hf_kafka_batch_compression: i32 = -1;
pub(crate) static mut hf_kafka_batch_timestamp_type: i32 = -1;
pub(crate) static mut hf_kafka_batch_istransactional: i32 = -1;
pub(crate) static mut hf_kafka_batch_iscontrolbatch: i32 = -1;
pub(crate) static mut hf_kafka_messageset_compression: i32 = -1;
pub(crate) static mut hf_kafka_messagest_timestamp_type: i32 = -1;

pub(crate) static mut kafka_batch_attributes: [*const i32; 5] = [
    unsafe {&hf_kafka_batch_compression as *const i32},
    unsafe {&hf_kafka_batch_timestamp_type as *const i32},
    unsafe {&hf_kafka_batch_istransactional as *const i32},
    unsafe {&hf_kafka_batch_iscontrolbatch as *const i32},
    0 as *const i32,
];

pub(crate) static mut kafka_messageset_attributes: [*const i32; 3] = [
    unsafe {&hf_kafka_messageset_compression as *const i32},
    unsafe {&hf_kafka_messagest_timestamp_type as *const i32},
    0 as *const i32,
];


pub(crate) fn hf2() -> Vec<hf_register_info> {vec![
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_batch_compression as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Compression\0"),
            abbrev: i8_str("kafka.recordbatch.attributes.compression\0"),
            type_: ftenum_FT_INT16,
            display: field_display_e_BASE_DEC as i32,
            strings: COMPRESSION_NAMES.as_ptr() as *const c_void,
            bitmask: 0b11,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_batch_timestamp_type as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Timestamp type\0"),
            abbrev: i8_str("kafka.recordbatch.attributes.timestamptype\0"),
            type_: ftenum_FT_INT16,
            display: field_display_e_BASE_DEC as i32,
            strings: TIMESTAMP_TYPE_NAMES.as_ptr() as *const c_void,
            bitmask: 0b100,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_batch_istransactional as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Is transactional\0"),
            abbrev: i8_str("kafka.recordbatch.attributes.istransactional\0"),
            type_: ftenum_FT_INT16,
            display: field_display_e_BASE_DEC as i32,
            strings: 0 as *const c_void,
            bitmask: 0b1000,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_batch_iscontrolbatch as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Is control batch\0"),
            abbrev: i8_str("kafka.recordbatch.attributes.iscontrolbatch\0"),
            type_: ftenum_FT_INT16,
            display: field_display_e_BASE_DEC as i32,
            strings: 0 as *const c_void,
            bitmask: 0b1_0000,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
    // Message Set attributes (are u8 instead of u16)
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_messageset_compression as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Compression\0"),
            abbrev: i8_str("kafka.messageset.attributes.compression\0"),
            type_: ftenum_FT_INT8,
            display: field_display_e_BASE_DEC as i32,
            strings: COMPRESSION_NAMES.as_ptr() as *const c_void,
            bitmask: 0b11,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
    hf_register_info {
        p_id: unsafe { &mut hf_kafka_messagest_timestamp_type as *mut _ },
        hfinfo: header_field_info {
            name: i8_str("Timestamp type\0"),
            abbrev: i8_str("kafka.messageset.attributes.timestamptype\0"),
            type_: ftenum_FT_INT8,
            display: field_display_e_BASE_DEC as i32,
            strings: TIMESTAMP_TYPE_NAMES.as_ptr() as *const c_void,
            bitmask: 0b100,
            blurb: 0 as *const i8,
            id: -1,
            parent: 0,
            ref_type: hf_ref_type_HF_REF_TYPE_NONE,
            same_name_prev_id: -1,
            same_name_next: 0 as *mut _header_field_info,
        }
    },
]}

static COMPRESSION_NAMES : [value_string; 6] = [
    value_string { value: 0_u32, strptr: i8_str("none\0")},
    value_string { value: 1_u32, strptr: i8_str("gzip\0")},
    value_string { value: 2_u32, strptr: i8_str("snappy\0")},
    value_string { value: 3_u32, strptr: i8_str("lz4\0")},
    value_string { value: 4_u32, strptr: i8_str("zstd\0")},
    // TODO: need `value_string as null`
    value_string { value: 0_u32, strptr: 0 as *const i8},
];

static TIMESTAMP_TYPE_NAMES : [value_string; 3] = [
    value_string { value: 0_u32, strptr: i8_str("Create\0")},
    value_string { value: 1_u32, strptr: i8_str("LogAppend\0")},
    value_string { value: 0_u32, strptr: 0 as *const i8},
];


// hf
/*
pub(crate) static mut hf_kafka_support_min_version: i32 = -1;
pub(crate) static mut hf_kafka_support_max_version: i32 = -1;
pub(crate) static mut hf_kafka_broker_host: i32 = -1;
*/


lazy_static! {
    // TODO: should they terminate with null tuple?
    pub(crate) static ref kafka_api_names: Vec<value_string> = api_keys
        .iter()
        .map(|(k, v)| {
            value_string {
                value: *k as u32,
                strptr: v.as_ptr() as *const i8,
            }
        })
        .collect();

    pub(crate) static ref kafka_error_names: Vec<value_string> = kafka_errors
        .iter()
        .map(|(k, v)| {
            value_string {
                value: *k as u32,
                strptr: v.as_ptr() as *const i8,
            }
        })
        .collect();

}



/*
lazy_static! {
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_support_min_version as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("Min Version\0"),
                abbrev: i8_str("kafka.api_versions.min_version\0"),
                type_: ftenum_FT_INT16,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("Minimal version which supports api key.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_support_max_version as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("Max Version\0"),
                abbrev: i8_str("kafka.api_versions.max_version\0"),
                type_: ftenum_FT_INT16,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("Maximum version which supports api key.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
    ]);
}
*/

pub(crate) static api_keys: [(u16, &'static str); 43] = [
    (0, "Produce\0"),
    (1, "Fetch\0"),
    (2, "ListOffsets\0"),
    (3, "Metadata\0"),
    (4, "LeaderAndIsr\0"),
    (5, "StopReplica\0"),
    (6, "UpdateMetadata\0"),
    (7, "ControlledShutdown\0"),
    (8, "OffsetCommit\0"),
    (9, "OffsetFetch\0"),
    (10, "FindCoordinator\0"),
    (11, "JoinGroup\0"),
    (12, "Heartbeat\0"),
    (13, "LeaveGroup\0"),
    (14, "SyncGroup\0"),
    (15, "DescribeGroups\0"),
    (16, "ListGroups\0"),
    (17, "SaslHandshake\0"),
    (18, "ApiVersions\0"),
    (19, "CreateTopics\0"),
    (20, "DeleteTopics\0"),
    (21, "DeleteRecords\0"),
    (22, "InitProducerId\0"),
    (23, "OffsetForLeaderEpoch\0"),
    (24, "AddPartitionsToTxn\0"),
    (25, "AddOffsetsToTxn\0"),
    (26, "EndTxn\0"),
    (27, "WriteTxnMarkers\0"),
    (28, "TxnOffsetCommit\0"),
    (29, "DescribeAcls\0"),
    (30, "CreateAcls\0"),
    (31, "DeleteAcls\0"),
    (32, "DescribeConfigs\0"),
    (33, "AlterConfigs\0"),
    (34, "AlterReplicaLogDirs\0"),
    (35, "DescribeLogDirs\0"),
    (36, "SaslAuthenticate\0"),
    (37, "CreatePartitions\0"),
    (38, "CreateDelegationToken\0"),
    (39, "RenewDelegationToken\0"),
    (40, "ExpireDelegationToken\0"),
    (41, "DescribeDelegationToken\0"),
    (42, "DeleteGroups\0"),
];

pub(crate) static kafka_errors: [(i32, &'static str); 78] = [
    (-1, "UNKNOWN_SERVER_ERROR\0"),
    (0, "NONE\0"),
    (1, "OFFSET_OUT_OF_RANGE\0"),
    (2, "CORRUPT_MESSAGE\0"),
    (3, "UNKNOWN_TOPIC_OR_PARTITION\0"),
    (4, "INVALID_FETCH_SIZE\0"),
    (5, "LEADER_NOT_AVAILABLE\0"),
    (6, "NOT_LEADER_FOR_PARTITION\0"),
    (7, "REQUEST_TIMED_OUT\0"),
    (8, "BROKER_NOT_AVAILABLE\0"),
    (9, "REPLICA_NOT_AVAILABLE\0"),
    (10, "MESSAGE_TOO_LARGE\0"),
    (11, "STALE_CONTROLLER_EPOCH\0"),
    (12, "OFFSET_METADATA_TOO_LARGE\0"),
    (13, "NETWORK_EXCEPTION\0"),
    (14, "COORDINATOR_LOAD_IN_PROGRESS\0"),
    (15, "COORDINATOR_NOT_AVAILABLE\0"),
    (16, "NOT_COORDINATOR\0"),
    (17, "INVALID_TOPIC_EXCEPTION\0"),
    (18, "RECORD_LIST_TOO_LARGE\0"),
    (19, "NOT_ENOUGH_REPLICAS\0"),
    (20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND\0"),
    (21, "INVALID_REQUIRED_ACKS\0"),
    (22, "ILLEGAL_GENERATION\0"),
    (23, "INCONSISTENT_GROUP_PROTOCOL\0"),
    (24, "INVALID_GROUP_ID\0"),
    (25, "UNKNOWN_MEMBER_ID\0"),
    (26, "INVALID_SESSION_TIMEOUT\0"),
    (27, "REBALANCE_IN_PROGRESS\0"),
    (28, "INVALID_COMMIT_OFFSET_SIZE\0"),
    (29, "TOPIC_AUTHORIZATION_FAILED\0"),
    (30, "GROUP_AUTHORIZATION_FAILED\0"),
    (31, "CLUSTER_AUTHORIZATION_FAILED\0"),
    (32, "INVALID_TIMESTAMP\0"),
    (33, "UNSUPPORTED_SASL_MECHANISM\0"),
    (34, "ILLEGAL_SASL_STATE\0"),
    (35, "UNSUPPORTED_VERSION\0"),
    (36, "TOPIC_ALREADY_EXISTS\0"),
    (37, "INVALID_PARTITIONS\0"),
    (38, "INVALID_REPLICATION_FACTOR\0"),
    (39, "INVALID_REPLICA_ASSIGNMENT\0"),
    (40, "INVALID_CONFIG\0"),
    (41, "NOT_CONTROLLER\0"),
    (42, "INVALID_REQUEST\0"),
    (43, "UNSUPPORTED_FOR_MESSAGE_FORMAT\0"),
    (44, "POLICY_VIOLATION\0"),
    (45, "OUT_OF_ORDER_SEQUENCE_NUMBER\0"),
    (46, "DUPLICATE_SEQUENCE_NUMBER\0"),
    (47, "INVALID_PRODUCER_EPOCH\0"),
    (48, "INVALID_TXN_STATE\0"),
    (49, "INVALID_PRODUCER_ID_MAPPING\0"),
    (50, "INVALID_TRANSACTION_TIMEOUT\0"),
    (51, "CONCURRENT_TRANSACTIONS\0"),
    (52, "TRANSACTION_COORDINATOR_FENCED\0"),
    (53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED\0"),
    (54, "SECURITY_DISABLED\0"),
    (55, "OPERATION_NOT_ATTEMPTED\0"),
    (56, "KAFKA_STORAGE_ERROR\0"),
    (57, "LOG_DIR_NOT_FOUND\0"),
    (58, "SASL_AUTHENTICATION_FAILED\0"),
    (59, "UNKNOWN_PRODUCER_ID\0"),
    (60, "REASSIGNMENT_IN_PROGRESS\0"),
    (61, "DELEGATION_TOKEN_AUTH_DISABLED\0"),
    (62, "DELEGATION_TOKEN_NOT_FOUND\0"),
    (63, "DELEGATION_TOKEN_OWNER_MISMATCH\0"),
    (64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED\0"),
    (65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED\0"),
    (66, "DELEGATION_TOKEN_EXPIRED\0"),
    (67, "INVALID_PRINCIPAL_TYPE\0"),
    (68, "NON_EMPTY_GROUP\0"),
    (69, "GROUP_ID_NOT_FOUND\0"),
    (70, "FETCH_SESSION_ID_NOT_FOUND\0"),
    (71, "INVALID_FETCH_SESSION_EPOCH\0"),
    (72, "LISTENER_NOT_FOUND\0"),
    (73, "TOPIC_DELETION_DISABLED\0"),
    (74, "FENCED_LEADER_EPOCH\0"),
    (75, "UNKNOWN_LEADER_EPOCH\0"),
    (76, "UNSUPPORTED_COMPRESSION_TYPE\0"),
];