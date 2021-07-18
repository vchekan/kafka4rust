use super::api::*;
use crate::error::{BrokerFailureSource};
use crate::protocol::ErrorCode;
use crate::zigzag::*;
use crate::error::BrokerResult;
use bytes::{Buf, BufMut, BytesMut, Bytes};
use std::fmt::Debug;
use anyhow::anyhow;

//
// Primitive types serializtion
//
impl ToKafka for u32 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u32(*self);
    }
}

impl ToKafka for i32 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i32(*self);
    }
}

impl ToKafka for u64 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u64(*self);
    }
}

impl ToKafka for i64 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i64(*self);
    }
}

impl ToKafka for u16 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u16(*self);
    }
}

impl ToKafka for i16 {
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_i16(*self);
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
        buff.put_u16(self.len() as u16);
        buff.put_slice(self.as_bytes());
    }
}

impl<T> ToKafka for Vec<T>
where
    T: ToKafka,
{
    fn to_kafka(&self, buff: &mut BytesMut) {
        buff.put_u32(self.len() as u32);
        for s in self {
            s.to_kafka(buff);
        }
    }
}

//
// Primitive types deserialization
//
impl FromKafka for String {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 4 {
            return Err(BrokerFailureSource::Serialization(anyhow!("EOF while reading string len")));
        }
        let size = buff.get_u16() as usize;
        if buff.remaining() < size {
            return Err(BrokerFailureSource::Serialization(anyhow!("EOF while reading string")));
        }
        let str = String::from_utf8(buff.get(..size).unwrap().to_vec())
            .map_err(|e| BrokerFailureSource::Serialization(e.into()))?;
        buff.advance(size);
        Ok(str)
    }
}

impl FromKafka for u32 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 4 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_u32())
    }
}

impl FromKafka for i32 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 4 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_i32())
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 8 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_u64())
    }
}

impl FromKafka for i64 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 8 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_i64())
    }
}

impl FromKafka for u16 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 2 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_u16())
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 2 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        Ok(buff.get_i16())
    }
}

impl<T> FromKafka for Vec<T>
where
    T: FromKafka + Debug,
{
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 4 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        let len = buff.get_i32();
        if len == -1 || len == 0 {
            return Ok(vec![]);
        }

        if buff.remaining() < len as usize {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let t = T::from_kafka(buff)?;
            res.push(t);
        }
        Ok(res)
    }
}

impl FromKafka for ErrorCode {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        if buff.remaining() < 2 {
            return Err(BrokerFailureSource::Serialization(anyhow!("Unexpected end of buffer")));
        }
        let code = buff.get_i16();
        // TODO: how to avoid unsafe? Also, in case ErrorCode is appended new error codes, this check will fail.
        if code >= -1 && code <= 87 {
            Ok(unsafe { std::mem::transmute(code) })
        } else {
            Err(BrokerFailureSource::Serialization(anyhow!("Invalid error code")))
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
    pub fn last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }
}

impl FromKafka for Recordset {
    fn from_kafka(buff: &mut Bytes) -> BrokerResult<Self> {
        // TODO: skip control batches

        if buff.remaining() < 4 {
            return Err(BrokerFailureSource::Serialization(anyhow!("EOF while reading segment size")));
        }

        let segment_size = buff.get_u32();
        if segment_size == 0 {
            // empty recordset
            return Ok(Recordset {
                base_offset: 0,
                last_offset_delta: 0,
                messages: vec![],
            });
        }

        // TODO: assert buff size
        let magic = buff.get(8 + 4 + 4);
        match magic {
            Option::None => {
                return Err(BrokerFailureSource::Serialization(anyhow!("EOF reading magic")));
            }
            Some(2) => {}
            Some(magic) => {
                return Err(BrokerFailureSource::Serialization(anyhow!("unexpectedf magic: {}", *magic)));
            }
        };

        let base_offset = buff.get_i64() as u64;
        // TODO: should I apply additional len-restricted view?
        let _batch_len = buff.get_u32();
        let _partition_leader_epoch = buff.get_u32();
        buff.get_u8(); // skip magic, we've checked it already
                       // TODO: check crc
        let _crc = buff.get_u32();
        let _attributes = buff.get_u16();
        let last_offset_delta = buff.get_u32();
        let _first_timestamp = buff.get_u64();
        let _max_timestamp = buff.get_u64();
        let _producer_id = buff.get_u64();
        let _producer_epoch = buff.get_u16();
        let _base_sequence = buff.get_u32();

        let records_len = buff.get_u32();
        let mut recordset = Recordset {
            base_offset,
            last_offset_delta,
            messages: vec![],
        };
        for _ in 0..records_len {
            let len = get_zigzag64(buff);
            if buff.remaining() < len as usize {
                return Err(BrokerFailureSource::Serialization(anyhow!("corrupt recordset: EOF")));
            }
            let _attributes = buff.get_u8();
            let _timestamp_delta = get_zigzag64(buff);
            let _offset_delta = get_zigzag64(buff);

            let key_len = get_zigzag64(buff);
            let _key = if key_len <= 0 {
                vec![]
            } else {
                buff.get(0..key_len as usize).unwrap().to_owned()
            };
            if key_len > 0 {
                buff.advance(key_len as usize);
            }

            let val_len = get_zigzag64(buff);
            let val = if val_len <= 0 {
                vec![]
            } else {
                buff.get(0..val_len as usize).unwrap().to_owned()
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
