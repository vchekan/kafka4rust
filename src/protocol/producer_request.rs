use crate::types::QueuedMessage;
use crate::protocol::{ApiKey, HasApiKey, HasApiVersion, ProduceResponse3, Request, ToKafka};
use crate::zigzag::{put_zigzag64, zigzag_len};
use bytes::{BufMut, BytesMut};
use crc32c::crc32c;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

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

pub(crate) struct ProduceRequest3<'a> {
    pub transactional_id: Option<&'a str>,
    pub acks: i16, // 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub timeout: i32, // The time to await a response in ms
    pub topic_data:
        &'a HashMap<&'a str, HashMap<u32, (&'a [QueuedMessage], &'a [QueuedMessage])>>,
}

impl HasApiKey for ProduceRequest3<'_> {
    fn api_key() -> ApiKey {
        ApiKey::Produce
    }
}

impl Request for ProduceRequest3<'_> {
    type Response = ProduceResponse3;
}

impl HasApiVersion for ProduceRequest3<'_> {
    fn api_version() -> u16 {
        3
    }
}

impl ToKafka for ProduceRequest3<'_> {
    fn to_kafka(&self, buff: &mut BytesMut) {
        self.serialize(buff);
    }
}

impl ProduceRequest3<'_> {
    pub(crate) fn serialize(&self, buf: &mut BytesMut) {
        serialize_string_opt(&self.transactional_id, buf);

        buf.reserve(2 + 4);
        buf.put_i16(self.acks);
        buf.put_i32(self.timeout);

        buf.reserve(4);
        buf.put_u32(self.topic_data.len() as u32);
        for (topic, data) in self.topic_data {
            buf.reserve(2);
            buf.put_u16(topic.len() as u16);
            buf.extend_from_slice(topic.as_bytes());

            buf.reserve(4);
            buf.put_u32(data.len() as u32);
            for (partition, (recordset1, recordset2)) in data {
                buf.reserve(4);
                buf.put_u32(*partition);

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
                    + 4 // base sequence
                    + 4, // recordset size
                );

                let recordset_bookmark = buf.len();
                buf.put_u32(0);

                buf.put_slice(&ZERO64); // base offset
                let batch_len_bookmark = buf.len();
                buf.put_slice(&ZERO32);
                buf.put_slice(&MINUS_ONE32); // partition leader epoch
                buf.put_u8(2); // magic
                let crc_bookmark = buf.len();
                buf.put_slice(&ZERO32); // crc, will calculate at the end
                buf.put_u16(
                    CompressionType::None as u16    // TODO
                    | TimestampType::Create as u16,
                );
                buf.put_u32((recordset1.len() + recordset2.len()) as u32 - 1); // last offset delta

                // TODO: timestamp messages and get timestamp from the first one
                // TODO: if timestamp is client generated, it is possible it will be negative.
                //  Should we find min or encode signed i64?
                // TODO: take into account, is timestamp client or log type.
                let first_timestamp = recordset1
                    .first()
                    .or_else(|| recordset2.first())
                    .expect("Empty recordset")
                    .timestamp;
                buf.put_u64(first_timestamp);
                // TODO: max timestamp
                buf.put_u64(first_timestamp);
                buf.put_slice(&MINUS_ONE64); // producer id
                buf.put_slice(&MINUS_ONE16); // producer epoch
                buf.put_slice(&MINUS_ONE32); // base sequence

                // records array
                let rs_len = recordset1.len() + recordset2.len();
                buf.put_u32(rs_len as u32);
                assert!(rs_len > 0, "Empty recordset");
                for (i, record) in recordset1.iter().enumerate() {
                    write_record(buf, i as u64, record.timestamp - first_timestamp, record)
                }
                for (i, record) in recordset2.iter().enumerate() {
                    write_record(buf, i as u64, record.timestamp - first_timestamp, record)
                }

                // write data and batch size
                let recordset_len = buf.len() - recordset_bookmark - 4;
                buf[recordset_bookmark..][..4].copy_from_slice(&(recordset_len as u32).to_be_bytes());
                let batch_len = buf.len() - batch_len_bookmark - 4;
                buf[batch_len_bookmark..][..4].copy_from_slice(&(batch_len as u32).to_be_bytes());

                // Calculate Crc after all length are set
                let crc = crc32c(&buf[crc_bookmark + 4..]);
                buf[crc_bookmark..][..4].copy_from_slice(&crc.to_be_bytes());
            }
        }
    }
}

impl Debug for ProduceRequest3<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // write!(f, "ProduceRequest3 [transactional_id: {:?}], asks: {:?}", self.transactional_id, self.acks)
        f.debug_struct("ProduceRequest3")
            .field("transactional_id", &self.transactional_id)
            .field("asks", &self.acks)
            .field("timeout", &self.timeout)
            .field("topics_data(length)", &self.topic_data.len())
            .finish()
    }
}

fn write_record(buf: &mut BytesMut, offset_delta: u64, timestamp_delta: u64, msg: &QueuedMessage) {
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

    buf.put_slice(put_zigzag64(len, &mut varint_buf));
    buf.put_u8(0); // attributes
    buf.put_slice(put_zigzag64(timestamp_delta, &mut varint_buf));
    buf.put_slice(put_zigzag64(offset_delta, &mut varint_buf));
    match &msg.key {
        Some(key) => {
            buf.put_slice(put_zigzag64(key_len as u64, &mut varint_buf));
            buf.put_slice(key);
        }
        None => buf.put_u8(VARINT_MINUS_ONE),
    }
    buf.put_slice(put_zigzag64(msg.value.len() as u64, &mut varint_buf));
    buf.put_slice(&msg.value);
    buf.put_u8(0); // TODO: headers
}

fn serialize_string_opt(s: &Option<&str>, buf: &mut BytesMut) {
    match s {
        Some(tx) => {
            buf.reserve(2 + tx.len());
            buf.put_u16(tx.len() as u16);
            buf.put_slice(tx.as_bytes());
        }
        None => {
            buf.reserve(2);
            buf.put_slice(&MINUS_ONE16);
        }
    }
}
