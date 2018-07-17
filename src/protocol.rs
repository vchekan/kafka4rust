use bytes::{BufMut, Buf, ByteOrder};
use byteorder::BigEndian;

//
// Public API
//
pub trait ToKafka {
    fn to_kafka(&self, buff: &mut BufMut);
}

pub trait FromKafka {
    fn from_kafka(buff: &mut Buf) -> Self;
}

pub trait ApiKey {
    fn api_key() -> u16;
}

pub trait ApiVersion {
    fn api_version() -> u16;
}

pub fn write_request<T>(request: &T, correlation_id: u32, client_id: Option<&str>, buff: &mut Vec<u8>)
    where T: ToKafka + ApiKey + ApiVersion
{
    buff.clear();
    buff.put_u32_be(0); // Size: will fix after message is serialized
    buff.put_u16_be(T::api_key());
    buff.put_u16_be(T::api_version());
    buff.put_u32_be(correlation_id);
    client_id.unwrap_or("k4r").to_kafka(buff);
    request.to_kafka(buff);

    // fix message size
    let size = buff.len() - 4;
    BigEndian::write_u32(&mut buff[0..4], size as u32);
}

pub fn read_response<T>(buff: &mut Buf) -> (u32,T) where T: FromKafka {
    let corr_id = buff.get_u32_be();
    let response = T::from_kafka(buff);
    (corr_id, response)
}

//
// Private
//
impl ApiKey for MetadataRequest0 { fn api_key() -> u16 { 3 } }
impl ApiKey for ListGroupRequest0 { fn api_key() -> u16 { 16 } }
impl ApiKey for ApiVersionsRequest { fn api_key() -> u16 { 18 } }

impl ApiVersion for MetadataRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ListGroupRequest0 { fn api_version() -> u16 { 0 } }
// TODO: one struct, 2 versions
//impl ApiVersion for ApiVersionsRequest0 { fn api_version() -> u16 { 0 } }
impl ApiVersion for ApiVersionsRequest { fn api_version() -> u16 { 1 } }

//
// Primitive types serializtion
//
impl ToKafka for u32 {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u32_be(*self);
    }
}

impl ToKafka for u64 {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u64_be(*self);
    }
}

impl ToKafka for String {
    fn to_kafka(&self, buff: &mut BufMut) {
        self.as_str().to_kafka(buff);
    }
}

impl ToKafka for str {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u16_be(self.len() as u16);
        buff.put_slice(self.as_bytes());
    }
}

impl<T> ToKafka for Vec<T> where T: ToKafka {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u32_be(self.len() as u32);
        for s in self {
            s.to_kafka(buff);
        }
    }
}

//
// Primitive types deserialization
//
impl FromKafka for String {
    fn from_kafka(buff: &mut Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let size = buff.get_u16_be() as usize;
        assert!(buff.remaining() >= size);
        // TODO: error handling
        String::from_utf8(buff.bytes()[..size].to_vec()).unwrap()
    }
}

impl FromKafka for u32 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u32_be()
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u64_be()
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_i16_be()
    }
}

impl<T> FromKafka for Vec<T> where T: FromKafka {
    fn from_kafka(buff: &mut Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let len = buff.get_u32_be();
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            res.push(T::from_kafka(buff));
        }
        res
    }
}

//
// Request macros
//
macro_rules! get_type {
    ([$t:ident $body:tt] ) => (Vec<get_type!($t)>);
    ($t:ident $body:tt) => ($t);
    ($t:ident) => ($t);
    ([$t:ident]) => (Vec<$t>);
}

macro_rules! request {
    ($id:ident) => {};
    ( [$id:ident] ) => {};

    // Array of complex type
    ( [$sname:ident $tp:tt]) => {request!($sname $tp);};

    ($sname:ident { $($f:ident : $tp:tt),* } ) => {
        pub struct $sname {
            $(pub $f : get_type!($tp) ),*
        }

        impl ToKafka for $sname {
            fn to_kafka(&self, buff: &mut BufMut) {
                $(self.$f.to_kafka(buff);)*
            }
        }

        $(request!($tp);)*
    };
}

macro_rules! response {
    ($id:ident) => {};
    ( [$id:ident] ) => {};

    // Array of complex type
    ( [ $sname:ident $tp:tt ] ) => (response!($sname $tp););


    ($sname:ident { $($f:ident : $tp:tt),* }) => {
        #[derive(Debug)]
        pub struct $sname {
            $($f: get_type!($tp) ),*
        }
        
        impl FromKafka for $sname {
            fn from_kafka(buff: &mut Buf) -> $sname {
                $sname { $($f: <get_type!($tp)>::from_kafka(buff)),* }
            }
        }

        $( response!($tp); )*
    };
}

//
// Request
//
/*
*/
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
    topic_name: [String]
});


request!(ListGroupRequest0{});

//
// Response
//

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

trace_macros!(true);
response!(ApiVersionsResponse1
    { error_code: i16
    , api_versions: [ApiVersions]
    , throttle_time_ms: u32
    }
);
trace_macros!(false);