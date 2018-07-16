use bytes::{BufMut, ByteOrder};
use byteorder::BigEndian;

//
// Public API
//
pub enum Request {
    Metadata(MetadataRequest),
    ListGroup(ListGroupRequest),
}

pub fn write_request(request: &Request, correlation_id: u32, client_id: Option<&str>, buff: &mut Vec<u8>) {
    // todo:reserve space for message size and bookmark it
    /* RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
          ApiKey => int16
          ApiVersion => int16
          CorrelationId => int32
          ClientId => string
    */
    buff.clear();
    //RequestOrResponse => Size (RequestMessage | ResponseMessage)
    //  Size => int32
    buff.put_u32_be(0); // Size: will fix after message is serialized
    buff.put_u16_be(api_key(request));
    buff.put_u16_be(api_version(request));
    buff.put_u32_be(correlation_id);
    client_id.unwrap_or("k4r").serialize(buff);
    request.serialize(buff);

    // fix message size
    let size = buff.len() - 4;
    BigEndian::write_u32(&mut buff[0..4], size as u32);
}

//
// Private
//

fn api_key(r: &Request) -> u16 {
    match r {
        Request::Metadata(_) => 3,
        Request::ListGroup(_) => 16,
    }
}

/// Supported Api version
fn api_version(r: &Request) -> u16 {
    match r {
        Request::Metadata(_) => 0,
        Request::ListGroup(_) => 0,
    }
}

/// Kafka serialization trait
trait SerializeKafka {
    fn serialize(&self, buff: &mut BufMut);
}

//
// Primitive types serializtion
//
impl SerializeKafka for String {
    fn serialize(&self, buff: &mut BufMut) {
        self.as_str().serialize(buff);
    }
}

impl SerializeKafka for str {
    fn serialize(&self, buff: &mut BufMut) {
        buff.put_u16_be(self.len() as u16);
        buff.put_slice(self.as_bytes());
    }
}

impl SerializeKafka for Vec<String> {
    fn serialize(&self, buff: &mut BufMut) {
        buff.put_u32_be(self.len() as u32);
        for s in self {
            s.serialize(buff);
        }
    }
}

//
// Requests serializtion
//

impl SerializeKafka for Request {
    fn serialize(&self, buff: &mut BufMut) {
        match self {
            Request::Metadata(request) => request.serialize(buff),
            Request::ListGroup(request) => request.serialize(buff),
        }
    }
}

//
// Request macros
//
macro_rules! request {
    ($request_name:ident {
        $($field_name:ident : $field_type:ty);*
    }) => (
        pub struct $request_name {
            $($field_name: $field_type,).*
        }

        impl SerializeKafka for $request_name {
            fn serialize(&self, buff: &mut BufMut) {
                $(self.$field_name.serialize(buff);)*
            }
        }
    )
}

/*
MetadataRequest => [TopicName]
  TopicName => string

*/
//trace_macros!(true);
request!(MetadataRequest {
    topic_name: Vec<String>
});
//trace_macros!(false);


//trace_macros!(true);
request!(ListGroupRequest {

});
//trace_macros!(false);

