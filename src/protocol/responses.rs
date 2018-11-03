use super::api::*;
use bytes::{Buf};

#[derive(Debug)]
pub enum Responses {
    MetadataResponse0(MetadataResponse0),
    ApiVersionsResponse0(ApiVersionsResponse0)
}

// 3
response!(MetadataResponse0 {
    brokers: [Broker],
    topics: [TopicMetadata]
});
response!(Broker {
    node_id: i32,
    host: String,
    port: i32
});
response!(TopicMetadata{
    error_code: i16,
    topic: String,
    partition_metadata: [PartitionMetadata]
});
response!(PartitionMetadata{
    error_code: i16,
    partition: i32,
    leader: i32,
    replicas: i32,
    isr: i32
});

response!(ListOffsetsResponse0 {
    responses: [ Response {
        topic: String,
        partition_responses: [
            PartitionResponses {
                partition: u32,
                error_code: i16,
                offsets: [u64]
            }
        ]
    }]
});

response!(ListGroupResponse0 {
    error_code: i16,
    groups : [ Group
        { group_id: String
        , protocol_type: String
        }
    ]
});

// 18
response!(ApiVersionsResponse0 {
    error_code: i16,
    api_versions: [ApiVersions]
});

response!(ApiVersions
    { api_key: i16
    , min_version: i16
    , max_version: i16
    }
);

response!(ApiVersionsResponse1
    { error_code: i16
    , api_versions: [ApiVersions]
    , throttle_time_ms: u32
    }
);