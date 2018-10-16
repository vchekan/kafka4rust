#![feature(trace_macros)]
#![feature(nll)]
//#![feature(rust_2018_preview)]
#![feature(generators)]

mod cluster;
mod broker;
mod connection;
mod protocol;
mod consumer;
mod producer;

extern crate failure;
#[macro_use] extern crate failure_derive;

extern crate tokio;
extern crate tokio_io;
extern crate futures;

extern crate bytes;
extern crate byteorder;
#[macro_use] extern crate derive_builder;
#[macro_use] extern crate log;
