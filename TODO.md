* Try parallel-streams

* IPv6 support
* Instrumentation

- [x] Tokio
- [x] Serde
- [ ] Client API
  - [x] Producer
  - [x] Consumer
  * Admin
  * Transactions
- [ ] Integration testing
    * Test with different executors (std-async, tokio)
- [x] Open Tracing
    - [x] Use crate::tracing
- [x] Decide on error strategy. Use dynamic error?
* Wrappers
    - [x] C#
    - [ ] Java
* Enforce protocol response errors checks
- [x] Migrate off `failure` crate
    * enum + impl Error manually (in library)
    - [x] anyhow (recommended by withoutboats)
    * thiserror
    * eyre
    * snafu
* Tcp: nodelay, experiment with tx,rx buffer size
* Adaptive buffer size
* Trained dictionary for lz4
* Stats about producer/consumer buffers waiting, server throttling
* CI: do `cargo audit`
* Failure model: fail up to connection reset instead of panic.
* Use hashmap's raw_entry
* Validate that BytesMut has always enough reserve bytes. Or switch to Vec?

## Projects
* CLI tools
* Kafka mock: replay from file
- [x] Wireshark decoder
* Compaction and record batch analyzer
* Enterprise UI
* FS adapter