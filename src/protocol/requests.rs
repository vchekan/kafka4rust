use super::api::*;
use bytes::BufMut;
use protocol::responses::*;

// 0
request!(
    ProduceRequest0,
    ProduceResponse0 {
        acks: i16,    // 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
        timeout: i32, // The time to await a response in ms
        topic_data: [TopicProduceData]
    }
);
request!(TopicProduceData {
    topic: String,
    data: ProduceData
});
request!(ProduceData {
    partition: i32,
    record_set: [u8]
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
