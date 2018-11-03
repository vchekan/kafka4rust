use super::api::*;
use protocol::responses::*;
use bytes::{BufMut};

request!(ListOffsetsRequest0, ListOffsetsResponse0 {
    replica_id: u32,
     topics:
        [ Topics
            { topic: String
            , partitions:
                [ Partition
                    { partition: u32
                    , timestamp: u64
                    , max_num_offsets: u32
                    }
                ]
            }
        ]
    }
);

// 18, version 0 and 1
request!(ApiVersionsRequest0, ApiVersionsResponse0 {});
request!(ApiVersionsRequest1, ApiVersionsResponse1 {});

request!(MetadataRequest0, MetadataResponse0 {
    topics: [String]
});


request!(ListGroupRequest0, ListGroupResponse0 {});