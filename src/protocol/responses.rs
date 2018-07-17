use super::api::*;
use bytes::{Buf};

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

response!(ListGroupResponse {
    error_code: i16,
    groups : [ Group
        { group_id: String
        , protocol_type: String
        }
    ]
});

// 18
response!(ApiVersions
    { api_key: i16
    , min_version: i16
    , max_version: i16
    }
);

response!(ApiVersionsResponse0 {
    error_code: i16,
    api_versions: [ApiVersions]
});

response!(ApiVersionsResponse1
    { error_code: i16
    , api_versions: [ApiVersions]
    , throttle_time_ms: u32
    }
);
