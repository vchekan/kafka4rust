#![feature(async_await)]
#![feature(arbitrary_self_types)]
#![feature(async_closure)]

mod error;
mod protocol;
mod connection;
mod broker;
mod cluster;
mod producer;
//mod consumer;
mod zigzag;

extern crate failure;

extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate log;
