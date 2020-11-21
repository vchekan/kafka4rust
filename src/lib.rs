#![feature(arbitrary_self_types)]
#![feature(async_closure)]
#![feature(backtrace)]

mod types;
mod error;
pub mod protocol;
mod zigzag;
mod utils;
mod connection;
mod producer;
mod cluster;
mod broker;
mod consumer;
mod murmur2a;
mod futures;

#[macro_use]
extern crate log;

pub use self::consumer::{Consumer, ConsumerConfig};
pub use self::producer::{Producer, Response, BinMessage, StringMessage};
pub use self::error::KafkaError;
pub use self::cluster::Cluster;
