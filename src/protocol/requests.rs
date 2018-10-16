use super::api::*;
use protocol::responses::*;
use bytes::{BufMut};

pub enum Requests {
    //ListOffsetsRequest0,
    ApiVersionsRequest(ApiVersionsRequest0),
    MetadataRequest0(MetadataRequest0),
    //ListGroupRequest0,
}

/*impl ToKafka for Requests {
    fn to_kafka(&self, buff: &'_ mut BufMut) {
        match self {
            Requests::ApiVersionsRequest(req) => req.to_kafka(buff)
        }
    }
}*/

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
