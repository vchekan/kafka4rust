use crate::producer::QueuedMessage;
use crate::zigzag::{zigzag64, zigzag_len};
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use crc32c::crc32c;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use crate::protocol::{ApiKey, HasApiKey, Request, ProduceResponse0, HasApiVersion, ToKafka};
use crate::protocol::ApiKey::ApiVersions;

const ZERO32: [u8; 4] = [0, 0, 0, 0];
const ZERO64: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
const MINUS_ONE64: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
const MINUS_ONE32: [u8; 4] = [0xff, 0xff, 0xff, 0xff];
const MINUS_ONE16: [u8; 2] = [0xff, 0xff];
const VARINT_MINUS_ONE: u8 = 1;

#[repr(u16)]
enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

#[repr(u16)]
enum TimestampType {
    Create = 0,
    LogAppend = 1 << 3,
}

pub(crate) struct ProduceRequest0<'a> {
    pub acks: i16, // 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub timeout: i32, // The time to await a response in ms
    pub topic_data:
        &'a HashMap<&'a String, HashMap<u32, (&'a [QueuedMessage], &'a [QueuedMessage])>>,
}

impl HasApiKey for ProduceRequest0<'_> {
    fn api_key() -> ApiKey { ApiKey::Produce }
}

impl Request for ProduceRequest0<'_> {
    type Response = ProduceResponse0;
}

impl HasApiVersion for ProduceRequest0<'_> {
    fn api_version() -> u16 { 0 }
}


impl ToKafka for ProduceRequest0<'_> {
    fn to_kafka(&self, buff: &mut BytesMut) {
        self.serialize(buff);
    }
}


impl ProduceRequest0<'_> {
    pub(crate) fn serialize(&self, buf: &mut BytesMut) {
        // TODO: calc transaction Id len
        buf.reserve(2 + 4);

        buf.put_i16_be(self.acks);
        buf.put_i32_be(self.timeout);

        buf.reserve(4);
        buf.put_u32_be(self.topic_data.len() as u32);
        for (topic, data) in self.topic_data {
            buf.reserve(4);
            buf.put_u16_be(topic.len() as u16);
            buf.extend_from_slice(topic.as_bytes());

            buf.reserve(4);
            buf.put_u32_be(data.len() as u32);
            for (partition, (recordset1, recordset2)) in data {
                buf.reserve(4);
                buf.put_u32_be(*partition);

                //
                // Record batch
                //
                buf.reserve(
                    8  // base offset
                    + 4 // batch len
                    + 4 // partition leader epoch
                    + 1 // magic
                    + 4 // crc
                    + 2 // attributes
                    + 4 // last offset delta
                    + 8 // first timestamp
                    + 8 // max timestamp
                    + 8 // producer id
                    + 2 // producer epoch
                    + 4 //base sequence
                    + 4, // recordset size
                );

                let recordset_bookmark = buf.len();
                buf.put_u32_be(0);

                buf.put_slice(&ZERO64); // base offset
                let batch_len_bookmark = buf.len();
                buf.put_slice(&ZERO32);
                buf.put_slice(&MINUS_ONE32); // partition leader epoch
                buf.put_u8(2); // magic
                let crc_bookmark = buf.len();
                buf.put_slice(&ZERO32); // crc
                buf.put_u16_be(
                    CompressionType::None as u16    // TODO
                    | TimestampType::Create as u16,
                );
                buf.put_slice(&ZERO32); // last offset delta

                // TODO: timestamp messages and get timestamp from the first one
                // TODO: if timestamp is client generated, it is possible it will be negative.
                //  Should we find min or encode signed i64?
                // TODO: take into account, is timestamp client or log type.
                let first_timestamp = recordset1.first().or(recordset2.first()).
                    expect("Empty recordset").timestamp;
                buf.put_u64_be(first_timestamp);
                // TODO: max timestamp
                buf.put_u64_be(first_timestamp);
                buf.put_slice(&MINUS_ONE64); // producer id
                buf.put_slice(&MINUS_ONE16); // producer epoch
                buf.put_slice(&MINUS_ONE32); // base sequence

                // records array
                let rs_len = recordset1.len() + recordset2.len();
                buf.put_u32_be(rs_len as u32);
                assert!(rs_len > 0, "Empty recordset");
                for (i, record) in recordset1.iter().enumerate() {
                    mk_record(buf, i as u64, record.timestamp - first_timestamp, &record)
                }
                for (i, record) in recordset2.iter().enumerate() {
                    mk_record(buf, i as u64, record.timestamp - first_timestamp, &record)
                }

                // write data and batch size
                let recordset_len = buf.len() - recordset_bookmark - 4;
                BigEndian::write_u32(&mut buf[recordset_bookmark..], recordset_len as u32);
                let batch_len = buf.len() - batch_len_bookmark - 4;
                BigEndian::write_u32(&mut buf[batch_len_bookmark..], batch_len as u32);

                // Calculate Crc after all length are set
                let  crc = crc32c(&buf[crc_bookmark + 4..]);
                BigEndian::write_u32(&mut buf[crc_bookmark..], crc);
            }
        }
    }
}

fn mk_record(buf: &mut BytesMut, offset_delta: u64, timestamp_delta: u64, msg: &QueuedMessage) {
    let mut varint_buf = [0_u8; 9];
    let key_len = match &msg.key {
        Some(key) => key.len(),
        None => 0,
    };
    let len: u64 = 1 + // attr
        zigzag_len(timestamp_delta) as u64 +
        zigzag_len(offset_delta) as u64 +
        zigzag_len(key_len as u64) as u64 + key_len as u64+
        zigzag_len(msg.value.len() as u64) as u64 + msg.value.len() as u64+
        1; // TODO: headers
    buf.reserve(len as usize);

    buf.put_slice(zigzag64(len, &mut varint_buf));
    buf.put_u8(0); // attributes
    buf.put_slice(zigzag64(timestamp_delta, &mut varint_buf));
    buf.put_slice(zigzag64(offset_delta, &mut varint_buf));
    match &msg.key {
        Some(key) => {
            //buf.put_slice(zigzag64(key_len as u64, &mut varint_buf));
            //buf.put_slice(key);
            // TODO: fix key
            buf.put_u8(VARINT_MINUS_ONE);
        }
        None => buf.put_u8(VARINT_MINUS_ONE),
    }
    buf.put_slice(zigzag64(msg.value.len() as u64, &mut varint_buf));
    buf.put_slice(&msg.value);
    buf.put_u8(0); // TODO: headers
}
