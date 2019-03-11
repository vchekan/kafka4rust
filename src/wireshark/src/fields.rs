use crate::bindings::*;
use crate::utils::i8_str;
use lazy_static::lazy_static;
use spin::Mutex;
use std::os::raw::{c_char, c_int, c_void};

// hf
pub(crate) static mut hf_kafka_len: i32 = -1;
pub(crate) static mut hf_kafka_request_api_key: i32 = -1;
pub(crate) static mut hf_kafka_correlation_id: i32 = -1;
pub(crate) static mut hf_kafka_request_api_version: i32 = -1;
pub(crate) static mut hf_kafka_string_len: i32 = -1;
pub(crate) static mut hf_kafka_client_id: i32 = -1;

lazy_static! {
    pub(crate) static ref kafka_api_names: Vec<value_string> = api_keys
        .iter()
        .map(|(k, v)| {
            value_string {
                value: *k as u32,
                strptr: v.as_ptr() as *const i8,
            }
        })
        .collect();
}

lazy_static! {
    pub(crate) static ref HF: Mutex<[hf_register_info; 6]> = Mutex::new([
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_len as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("Length\0"),
                abbrev: i8_str("kafka.len\0"),
                type_: ftenum_FT_INT32,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("The length of this Kafka packet.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_request_api_key as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("API Key\0"),
                abbrev: i8_str("kafka.request_key\0"),
                type_: ftenum_FT_INT16,
                display: field_display_e_BASE_DEC as i32,
                strings: kafka_api_names.as_ptr() as *const c_void,
                bitmask: 0,
                blurb: i8_str("Request API.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_correlation_id as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("Correlation Id\0"),
                abbrev: i8_str("kafka.correlation_id\0"),
                type_: ftenum_FT_INT32,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: 0 as *const i8,
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_request_api_version as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("API Version\0"),
                abbrev: i8_str("kafka.request.version\0"),
                type_: ftenum_FT_INT16,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("Request API Version.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_string_len as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("String Length\0"),
                abbrev: i8_str("kafka.string_len\0"),
                type_: ftenum_FT_INT16,
                display: field_display_e_BASE_DEC as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("Generic length for kafka-encoded string.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
        hf_register_info {
            p_id: unsafe { &mut hf_kafka_client_id as *mut _ },
            hfinfo: header_field_info {
                name: i8_str("Client Id\0"),
                abbrev: i8_str("kafka.client_id\0"),
                type_: ftenum_FT_STRING,
                display: field_display_e_BASE_NONE as i32,
                strings: 0 as *const c_void,
                bitmask: 0,
                blurb: i8_str("The ID of the sending client.\0"),
                id: -1,
                parent: 0,
                ref_type: hf_ref_type_HF_REF_TYPE_NONE,
                same_name_prev_id: -1,
                same_name_next: 0 as *mut _header_field_info,
            }
        },
    ]);
}

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
