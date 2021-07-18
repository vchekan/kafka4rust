use crate::error::{BrokerResult, BrokerFailureSource};
use bytes::{BytesMut, Bytes};
use bytes::{Buf, BufMut};
use std::fmt::Debug;
use std::marker::Sized;
use crate::error::InternalError;

#[repr(u16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    ListGroup = 16,
    ApiVersions = 18,
}

pub trait ToKafka {
    fn to_kafka(&self, buff: &mut BytesMut);
}

// TODO: do I really need Buf trait or can use concrete MutBuf type?
pub trait FromKafka
where
    Self: Sized,
{
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self>;
}

pub trait HasApiKey {
    fn api_key() -> ApiKey;
}

pub trait HasApiVersion {
    fn api_version() -> u16;
}

pub trait Request: ToKafka + HasApiKey + HasApiVersion {
    type Response: FromKafka + Debug;
}

pub(crate) fn write_request<T>(
    request: &T,
    //correlation_id: u32,
    client_id: Option<&str>,
    buff: &mut BytesMut,
    correlation_id: u32,
) where
    T: Request,
{
    //let correlation_id = CORRELATION_ID.fetch_add(1, Ordering::SeqCst) as u32;
    buff.clear();
    buff.put_u32(0); // Size: will fix after message is serialized
    buff.put_u16(T::api_key() as u16);
    buff.put_u16(T::api_version());
    buff.put_u32(correlation_id);
    client_id
        .unwrap_or(crate::connection::CLIENT_ID)
        .to_kafka(buff);
    request.to_kafka(buff);

    // fix message size
    let size = buff.len() - 4;
    buff.get_mut(0..).unwrap().put_u32(size as u32);
}

fn test (e: BrokerFailureSource, ei: InternalError) {
    let e: anyhow::Error = anyhow::Error::new(e);
    let e = anyhow::Error::new(ei);
}

pub(crate) fn read_response<T>(buff: &mut Bytes) -> BrokerResult<(u32,T)>
where
    T: FromKafka,
{
    let corr_id = buff.get_u32();
    let response = T::from_kafka(buff)?;//.map_err(|e| {
        // let e: anyhow::Error = anyhow::Error::new(e);
        // InternalError::Serialization(e)
    // });
    Ok((corr_id, response))
}
