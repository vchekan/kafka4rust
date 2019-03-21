use super::FromKafka;
use bytes::{Buf, BufMut};
use std::fmt::Debug;

//
// Primitive types deserialization
//
impl FromKafka for String {
    fn from_kafka(buff: &mut Buf) -> Self {
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
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u32_be()
    }
}

impl FromKafka for i32 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_i32_be()
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u64_be()
    }
}

impl FromKafka for i64 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_i64_be()
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_i16_be()
    }
}

impl FromKafka for u8 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u8()
    }
}


impl<T> FromKafka for Vec<T>
where
    T: FromKafka + Debug,
{
    fn from_kafka(buff: &mut Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let len = buff.get_u32_be();
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let t = T::from_kafka(buff);
            res.push(t);
        }
        res
    }
}
