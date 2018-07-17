use bytes::{Buf, BufMut};
use super::api::*;


//
// Primitive types serializtion
//
impl ToKafka for u32 {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u32_be(*self);
    }
}

impl ToKafka for u64 {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u64_be(*self);
    }
}

impl ToKafka for String {
    fn to_kafka(&self, buff: &mut BufMut) {
        self.as_str().to_kafka(buff);
    }
}

impl ToKafka for str {
    fn to_kafka(&self, buff: &mut BufMut) {
        buff.put_u16_be(self.len() as u16);
        buff.put_slice(self.as_bytes());
    }
}

impl<T> ToKafka for Vec<T> where T: ToKafka {
    fn to_kafka(&self, buff: &mut BufMut) {
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
    fn from_kafka(buff: &mut Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let size = buff.get_u16_be() as usize;
        assert!(buff.remaining() >= size);
        // TODO: error handling
        String::from_utf8(buff.bytes()[..size].to_vec()).unwrap()
    }
}

impl FromKafka for u32 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u32_be()
    }
}

impl FromKafka for u64 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_u64_be()
    }
}

impl FromKafka for i16 {
    fn from_kafka(buff: &mut Buf) -> Self {
        buff.get_i16_be()
    }
}

impl<T> FromKafka for Vec<T> where T: FromKafka {
    fn from_kafka(buff: &mut Buf) -> Self {
        assert!(buff.remaining() >= 4);
        let len = buff.get_u32_be();
        let mut res = Vec::with_capacity(len as usize);
        for _ in 0..len {
            res.push(T::from_kafka(buff));
        }
        res
    }
}
