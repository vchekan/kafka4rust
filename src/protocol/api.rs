use bytes::{BufMut, Buf, ByteOrder};
use byteorder::BigEndian;
use std::fmt::Debug;

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

pub trait Request : ToKafka + ApiKey + ApiVersion {
    type Response : FromKafka + Debug;
}

pub fn write_request<T>(request: &T, correlation_id: u32, client_id: Option<&str>, buff: &mut Vec<u8>)
    where T: Request
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