# Kafka client (personal project, work in progress) 

## Focus
* [ ]  KIP-602: Change default value for client.dns.lookup


## Features
* IPv6 support
* Instrumentation

- [x] Tokio
- [x] Serde
- [ ] Recovery
  - [ ] Producer
  - [ ] Consumer
  - [X] Timeout
- [ ] Client API
  - [x] Producer
  - [x] Consumer
  - [ ] Compression
  - [ ] Headers
  - [ ] Admin
  - [ ] Transactions
  - [ ] SSL
- [ ] Integration testing
    - [ ] Test with different executors (std-async, tokio)
- [x] Open Tracing
    - [x] Use crate::tracing
* Wrappers
    - [x] C#
    - [x] Java
        - [ ] Learn Netty buffers and async
    - [ ] F#
    - [ ] Python
    - [ ] Rust https://willcrichton.net/rust-api-type-patterns/
* Enforce protocol response errors checks
- [x] Decide on error strategy. Use dynamic error?
- [x] Migrate off `failure` crate
    * enum + impl Error manually (in library)
    - [x] anyhow (recommended by withoutboats)
    * thiserror
    * eyre
    * snafu

## TODO
- [ ] Evaluate `select!` usage for cancellation safety: https://tomaka.medium.com/ (use tokio tcp streams)
- [ ] tokio-console
- [ ] Validate that BytesMut has always enough reserve bytes. Or switch to Vec?
- [X] Remove ByteOrder because `bytes` already have it  
- [ ] Make sure that command send and response are corresponding even in parallel scenario.
- [ ] Producer sent message size limit
a-look-back-at-asynchronous-rust-d54d63934a1c
  - Joshua's `Stream::merge`: https://blog.yoshuawuyts.com/futures-concurrency-3/#issues-with-futures-select 
- [ ] Implement BufferPool to minimize large vectors allocations  
- [ ] Use crossbeam::epoch::Pointable to store list of known brokers and metadata. Allows for lock-free walks over brokers.
- [ ] ~~Try actors approach: https://ryhl.io/blog/actors-with-tokio/~~
- [ ] Test when topic partition count changes (topic re-create)
- [ ] Are topics case-sensitive?
- [ ] Tracing conventions https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
- [ ] `async-backtrace` crate

## Techniques
* Try parallel-streams
* Consider `Cow<>` when deserializing.
* Consider `flume` for channels
* Considr `dashmap` for mutithreading access.
* Consider `slotmap` https://docs.rs/slotmap/1.0.1/slotmap/
* Consider `parking_lot` for non-poisoning locks.
* Consider `tinyvec` and `smolstr` for stack-optimized strings/arrays
* Consider using arena: https://manishearth.github.io/blog/2021/03/15/arenas-in-rust/  
* Audit that `copy` is used whenever possible, instead of `clone`
* Tcp: nodelay, experiment with tx,rx buffer size
* Adaptive buffer size
* Trained dictionary for lz4
* Stats about producer/consumer buffers waiting, server throttling
* CI: do `cargo audit`
* Failure model: fail up to connection reset instead of panic.
* ~~Use hashmap's raw_entry~~
  * Is unstable
* Try Async Local Executors
  * https://maciej.codes/2022-06-09-local-async.html
* `futures-concurrency`
* Glommio and Monoio are not Send and use a "thread-per-core" design
* https://docs.rs/thunderdome/latest/thunderdome/ address borrow checker pains and allows more natural code.

## Projects
* CLI tools
  - [ ] Read CLI guideline: https://github.com/cli-guidelines/cli-guidelines/blob/main/content/_index.md
  - [ ] Consider cafkacat compatible command line
  - [ ] Support avro, protobuf, thrift
* Kafka mock: replay from file
- [x] Wireshark decoder
* Compaction and record batch analyzer
* Enterprise UI
* FS adapter
* Server-side filtering

## Resources:
https://matklad.github.io/2020/10/15/study-of-std-io-error.html
https://willcrichton.net/rust-api-type-patterns/
https://aturon.github.io/blog/2015/08/27/epoch/
