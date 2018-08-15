use super::api::*;
use bytes::{BufMut};

pub enum Request {
    ListOffsetsRequest0,
    ApiVersionsRequest,
    MetadataRequest0,
    ListGroupRequest0,
}

request!(ListOffsetsRequest0 {
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
request!(ApiVersionsRequest {});

request!(MetadataRequest0 {
    topics: [String]
});


request!(ListGroupRequest0{});
