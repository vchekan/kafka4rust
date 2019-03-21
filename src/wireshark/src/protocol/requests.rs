use crate::bindings::*;
use super::FromKafka;
use crate::protocol::responses::*;
use bytes::BufMut;
use bytes::Buf;


trace_macros!(true);
request!(metadata_response => {
    Brokers: [Broker]
});

request!(Broker => {
    node_id: i32,
    host: String,
    port: i32
});
trace_macros!(false);
