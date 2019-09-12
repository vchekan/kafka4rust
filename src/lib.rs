#![feature(async_await, futures_api)]
#![feature(arbitrary_self_types)]

mod bichannel;
mod timer;
mod protocol;
//mod correlation_future;
mod connection;
mod broker;
//mod cluster;
//mod producer;
//mod consumer;
//mod futures_union;

extern crate failure;
#[macro_use]
extern crate failure_derive;

extern crate futures;

extern crate byteorder;
extern crate bytes;
extern crate derive_builder;
#[macro_use]
extern crate log;
extern crate simplelog;
