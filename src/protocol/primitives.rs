use super::api::*;
use bytes::{Buf, BytesMut, BufMut};
use std::fmt::Debug;
use crate::protocol::ErrorCode;
use crate::error::{Result, Error};
use crate::zigzag::*;
use failure::Fail;

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
            panic!("Invalid ErrorCode {}", code)
        }
    }
}

#[derive(Debug)]
// TODO: make it crate pub
pub struct Recordset {
    pub messages: Vec<Vec<u8>>,
}

impl FromKafka for Result<Recordset> {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        // TODO: skip control batches

        if buff.remaining() == 4 {
            // Empty recordset
            let segment_size = buff.get_u32_be();
            return match segment_size {
                0 => Ok(Recordset{messages: vec![]}),
                // Slice is empty, but size non-zero
                _ => Err(Error::CorruptMessage),
            };
        }

        let magic = buff.bytes().get(8+8+4);
        match magic { 
            Option::None => { return Err(Error::CorruptMessage); },
            Some(2) => {},
            Some(magic) => { return Err(Error::UnexpectedRecordsetMagic(*magic)); }
        };


        let segment_size = buff.get_u32_be();
        if segment_size == 0 {
            return Err(Error::CorruptMessage);
        }
        let base_offset = buff.get_i64_be();
        // TODO: should I apply additional len-restricted view?
        let batch_len = buff.get_u32_be();
        let partition_leader_epoch = buff.get_u32_be();
        buff.get_u8();  // skip magic, we've checked it already
        // TODO: check crc
        let crc = buff.get_u32_be();
        let attributes = buff.get_u16_be();
        let last_offset_delta = buff.get_u32_be();
        let first_timestamp = buff.get_u64_be();
        let max_timestamp = buff.get_u64_be();
        let producer_id = buff.get_u64_be();
        let producer_epoch = buff.get_u16_be();
        let base_sequence = buff.get_u32_be();

        let records_len = buff.get_u32_be();
        let mut recordset = Recordset{messages: vec![]};
        for _ in 0..records_len {
            let len = get_zigzag64(buff);
            if buff.remaining() < len as usize {
                return Err(Error::from(Error::CorruptMessage.context("Recordset deserialization")));
            }
            let _attributes = buff.get_u8();
            let timestamp_delta = get_zigzag64(buff);
            let offset_delta = get_zigzag64(buff);

            let key_len = get_zigzag64(buff);
            let _key = if key_len <= 0 {
                vec![]
            } else {
                buff.bytes()[0..key_len as usize].to_owned()
            };
            if key_len > 0 {
                buff.advance(key_len as usize);
            }

            let val_len = get_zigzag64(buff);
            let val = if val_len <= 0 {
                vec![]
            } else {
                buff.bytes()[0..val_len as usize].to_owned()
            };
            // Check just in case
            if val_len > 0 {
                buff.advance(val_len as usize);
            }

            recordset.messages.push(val.to_vec());
        }

        Ok(recordset)
    }
}