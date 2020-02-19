use super::api::*;
use bytes::{Buf, BytesMut, BufMut};
use std::fmt::Debug;
use crate::protocol::ErrorCode;

//
// Primitive types serializtion
//
impl ToKafka for u32 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u32_be(*self);
    }
}

impl ToKafka for i32 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i32_be(*self);
    }
}

impl ToKafka for u64 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u64_be(*self);
    }
}

impl ToKafka for i64 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i64_be(*self);
    }
}

impl ToKafka for u16 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u16_be(*self);
    }
}

impl ToKafka for i16 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i16_be(*self);
    }
}

impl ToKafka for u8 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u8(*self);
    }
}

impl ToKafka for String {
    fn to_kafka(&self, buff: &mut BytesMut) {
        self.as_str().to_kafka(buff);
    }
}

impl ToKafka for str {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u16_be(self.len() as u16);
        buff.put_slice(self.as_bytes());
    }
}

impl<T> ToKafka for Vec<T>
where
    T: ToKafka,
{
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u32_be(self.len() as u32);
        for s in self {
            s.to_kafka(buff);
        }
    }
}

/*
/// Byte array specialization
impl ToKafka for Vec<u8> {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_slice(&self);
    }
}
*/

//
// Primitive types deserialization
//
impl FromKafka for String {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let size = buff.get_u16_be() as usize;
        assert!(buff.remaining() >= size);
        // TODO: error handling
        let str = String::from_utf8(buff.bytes()[..size].to_vec()).unwrap();
        buff.advance(size);
        str
    }
}

impl FromKafka for u32 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_u32_be()
    }
}

impl FromKafka for i32 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_i32_be()
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_u64_be()
    }
}

impl FromKafka for i64 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_i64_be()
    }
}

impl FromKafka for u16 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_u16_be()
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        buff.get_i16_be()
    }
}

impl<T> FromKafka for Vec<T>
where
    T: FromKafka + Debug,
{
    fn from_kafka(buff: &mut impl Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let len = buff.get_i32_be();
        if len == -1 || len == 0 {
            return vec![];
        }
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let t = T::from_kafka(buff);
            res.push(t);
        }
        res
    }
}

impl FromKafka for ErrorCode {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        let code = buff.get_i16_be();
        if code >= -1 && code <= 87 {
            unsafe { std::mem::transmute(code) }
        } else {
            // TODO: change `FromKafka` api to return serialization error
            panic!()
        }
    }
}