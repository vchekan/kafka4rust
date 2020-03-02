use byteorder::BigEndian;
use bytes::{Buf, BufMut, ByteOrder};
use std::fmt::Debug;
use bytes::BytesMut;

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
pub trait FromKafka {
    fn from_kafka(buff: &mut impl Buf) -> Self;
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
    correlation_id: u32,
    client_id: Option<&str>,
    buff: &mut BytesMut,
) where
    T: Request,
{
    buff.clear();
    buff.put_u32_be(0); // Size: will fix after message is serialized
    buff.put_u16_be(T::api_key() as u16);
    buff.put_u16_be(T::api_version());
    buff.put_u32_be(correlation_id);
    client_id
        .unwrap_or(crate::connection::CLIENT_ID)
        .to_kafka(buff);
    request.to_kafka(buff);

    // fix message size
    let size = buff.len() - 4;
    BigEndian::write_u32(&mut buff[0..4], size as u32);
}

pub fn read_response<T>(buff: &mut impl Buf) -> (u32, T)
where
    T: FromKafka,
{
    let corr_id = buff.get_u32_be();
    let response = T::from_kafka(buff);
    (corr_id, response)
}

