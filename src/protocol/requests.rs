use super::api::*;
use crate::protocol::responses::*;
use bytes::BytesMut;

//
// 1 Fetch
//
request!(FetchRequest0, FetchResponse0 {
    replica_id: i32,
    max_wait_time: u32,
    min_bytes: u32,
    max_bytes: u32,                 // /3 : {hf_kafka_max_bytes: i32},
    isolation_level: u8,             // /4: {hf_kafka_isolation_level: u8},
    topics: [FetchTopic {
        topic: String,
        partitions: [FetchPartition {
            partition: u32,
            fetch_offset: i64,
            log_start_offset: i64,             // /5 : {hf_kafka_fetch_request_log_start_offset : i64},
            partition_max_bytes: u32
        }]
    }]
});

// 3
request!(MetadataRequest0, MetadataResponse0 { topics: [String] });

//
request!(
    ListOffsetsRequest0,
    ListOffsetsResponse0 {
        replica_id: u32,
        topics: [Topics {
            topic: String,
            partitions: [Partition {
                partition: u32,
                timestamp: u64,
                max_num_offsets: u32
            }]
        }]
    }
);

// 18, version 0 and 1
request!(ApiVersionsRequest0, ApiVersionsResponse0 {});
request!(ApiVersionsRequest1, ApiVersionsResponse1 {});

request!(ListGroupRequest0, ListGroupResponse0 {});
