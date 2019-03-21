#[macro_use]
mod macros;
mod primitives;
mod requests;
mod responses;

pub use self::requests::*;
pub use self::responses::*;

use bytes::Buf;

pub trait FromKafka {
    fn from_kafka(buff: &mut Buf) -> Self;
}