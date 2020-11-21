use super::api::*;
use bytes::{Buf, BytesMut, BufMut};
use std::fmt::Debug;
use crate::protocol::ErrorCode;
use crate::error::KafkaError;
use crate::zigzag::*;
use anyhow::Result;

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

//
// Primitive types deserialization
//
impl FromKafka for String {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 4 {
            return Err(KafkaError::CorruptMessage("EOF while reading a string len").into());
        }
        let size = buff.get_u16_be() as usize;
        if buff.remaining() < size {
            return Err(KafkaError::CorruptMessage("EOF while reading string").into());
        }
        let str = String::from_utf8(buff.bytes()[..size].to_vec())?;
        buff.advance(size);
        Ok(str)
    }
}

impl FromKafka for u32 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 4 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_u32_be())
    }
}

impl FromKafka for i32 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 4 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_i32_be())
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 8 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_u64_be())
    }
}

impl FromKafka for i64 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 8 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_i64_be())
    }
}

impl FromKafka for u16 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 2 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_u16_be())
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 2 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        Ok(buff.get_i16_be())
    }
}

impl<T> FromKafka for Vec<T>
where
    T: FromKafka + Debug,
{
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 4 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        let len = buff.get_i32_be();
        if len == -1 || len == 0 {
            return Ok(vec![]);
        }

        if buff.remaining() < len as usize { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let t = T::from_kafka(buff)?;
            res.push(t);
        }
        Ok(res)
    }
}

impl FromKafka for ErrorCode {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        if buff.remaining() < 2 { return Err(KafkaError::CorruptMessage("Unexpected end of buffer").into()); }
        let code = buff.get_i16_be();
        if code >= -1 && code <= 87 {
            Ok(unsafe { std::mem::transmute(code) })
        } else {
            Err(KafkaError::CorruptMessage("Invalid error code").into())
        }
    }
}

#[derive(Debug)]
// TODO: make it crate pub
pub struct Recordset {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub messages: Vec<Vec<u8>>,
}

impl Recordset {
    pub fn last_offset(&self) -> u64 { self.base_offset + self.last_offset_delta as u64 }
}

impl FromKafka for Recordset {
    fn from_kafka(buff: &mut impl Buf) -> Result<Self> {
        // TODO: skip control batches

        if buff.remaining() < 4 {
            return Err(KafkaError::CorruptMessage("EOF while reading segment size").into());
        }

        let segment_size = buff.get_u32_be();
        if segment_size == 0 {
            // empty recordset
            return Ok(Recordset {
                base_offset: 0,
                last_offset_delta: 0,
                messages: vec![]
            });
        }

        // TODO: assert buff size
        let magic = buff.bytes().get(8+4+4);
        match magic {
            Option::None => { return Err(KafkaError::CorruptMessage("EOF reading magic").into()); },
            Some(2) => {},
            Some(magic) => { return Err(KafkaError::UnexpectedRecordsetMagic(*magic).into()); }
        };
        
        let base_offset = buff.get_i64_be() as u64;
        // TODO: should I apply additional len-restricted view?
        let _batch_len = buff.get_u32_be();
        let _partition_leader_epoch = buff.get_u32_be();
        buff.get_u8();  // skip magic, we've checked it already
        // TODO: check crc
        let _crc = buff.get_u32_be();
        let _attributes = buff.get_u16_be();
        let last_offset_delta = buff.get_u32_be();
        let _first_timestamp = buff.get_u64_be();
        let _max_timestamp = buff.get_u64_be();
        let _producer_id = buff.get_u64_be();
        let _producer_epoch = buff.get_u16_be();
        let _base_sequence = buff.get_u32_be();

        let records_len = buff.get_u32_be();
        let mut recordset = Recordset{
            base_offset,
            last_offset_delta,
            messages: vec![]
        };
        for _ in 0..records_len {
            let len = get_zigzag64(buff);
            if buff.remaining() < len as usize {
                return Err(KafkaError::CorruptMessage("Recordset deserialization").into());
            }
            let _attributes = buff.get_u8();
            let _timestamp_delta = get_zigzag64(buff);
            let _offset_delta = get_zigzag64(buff);

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

            if val_len > 0 {
                buff.advance(val_len as usize);
            }

            // headers
            let h_len = get_zigzag64(buff);
            if h_len != 0 {
                unimplemented!()
            }

            recordset.messages.push(val.to_vec());
        }

        Ok(recordset)
    }
}