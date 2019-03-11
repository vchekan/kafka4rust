#![feature(async_await, await_macro, futures_api)]
#![feature(arbitrary_self_types)]
#![feature(mpsc_select)]

mod broker;
mod cluster;
mod connection;
mod producer;
mod protocol;
//mod consumer;
mod timer;
mod bichannel;
mod futures_union;

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
