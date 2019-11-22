use super::api::*;
use bytes::Buf;
use crate::error::{Result, Error};
use crate::zigzag::get_zigzag64;
use failure::Fail;
use failure::ResultExt;

#[derive(Debug)]
pub enum Responses {
    MetadataResponse0(MetadataResponse0),
    ApiVersionsResponse0(ApiVersionsResponse0),
}

// 0
response!(ProduceResponse0 {
    responses: [ProduceResponse]
});
response!(ProduceResponse {
    topic: String,
    partition_responses: [PartitionResponse]
});
response!(PartitionResponse {
    partition: i32,
    error_code: i16,
    base_offset: i64
});

// 1
response!(FetchResponse0 {
    throttle_time: i32,     // /1 : {hf_kafka_throttle_time_ms: i32},
    responses: [FetchResponse]
});

response!(FetchResponse {
    topic: String,
    partitions: [FetchPartitionResponse]
});

response!(FetchPartitionResponse {
    partition: u32,
    // TODO: error codes
    error_code: u16,
    high_watermark: u64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: [FetchAbortedTransactions],
    recordset: { fn parse_recordset() -> Result<Recordset> }
});

response!(FetchAbortedTransactions {
    producer_id: u64,
    first_offset: i64
});

// 3
response!(MetadataResponse0 {
    brokers: [Broker],
    topics: [TopicMetadata]
});
response!(Broker {
    node_id: i32,
    host: String,
    port: i32
});
response!(TopicMetadata {
    error_code: i16,
    topic: String,
    partition_metadata: [PartitionMetadata]
});
response!(PartitionMetadata {
    error_code: i16,
    partition: u32,
    leader: i32,
    replicas: i32,
    isr: i32
});

response!(ListOffsetsResponse0 {
    responses: [Response {
        topic: String,
        partition_responses: [PartitionResponses {
            partition: u32,
            error_code: i16,
            offsets: [u64]
        }]
    }]
});

response!(ListGroupResponse0 {
    error_code: i16,
    groups: [Group {
        group_id: String,
        protocol_type: String
    }]
});

// 18
response!(ApiVersionsResponse0 {
    error_code: i16,
    api_versions: [ApiVersions]
});

response!(ApiVersions {
    api_key: i16,
    min_version: i16,
    max_version: i16
});

response!(ApiVersionsResponse1 {
    error_code: i16,
    api_versions: [ApiVersions],
    throttle_time_ms: u32
});

#[derive(Debug)]
// TODO: make it crate pub
pub struct Recordset {
    messages: Vec<String>,
}

impl FromKafka for Result<Recordset> {
    fn from_kafka(buff: &mut impl Buf) -> Self {
        // TODO: skip control batches

        let magic = buff.bytes()[8+8+4];
        if magic != 2 {
            return Err(Error::UnexpectedRecordsetMagic(magic));
        }

        let segment_size = dbg!(buff.get_u32_be());
        let base_offset = dbg!(buff.get_i64_be());
        // TODO: should I apply additional len-restricted view?
        let batch_len = buff.get_u32_be();
        let partition_leader_epoch = buff.get_u32_be();
        buff.get_u8();  // skip magic, we've checked it already
        // TODO: check crc
        let crc = buff.get_u32_be();
        let attributes = buff.get_u16_be();
        let last_offset_delta = buff.get_u32_be();
        let first_timestamp = buff.get_u64_be();
        let max_timestamp = buff.get_u64_be();
        let producer_id = buff.get_u64_be();
        let producer_epoch = buff.get_u16_be();
        let base_sequence = buff.get_u32_be();

        let records_len = buff.get_u32_be();
        debug!("records_len: {}", records_len);
        let mut recordset = Recordset{messages: vec![]};
        for _ in 0..records_len {
            let len = get_zigzag64(buff);
            if buff.remaining() < len as usize {
                return Err(Error::from(Error::CorruptMessage.context("Recordset deserialization")));
            }
            let _attributes = buff.get_u8();
            let timestamp_delta = get_zigzag64(buff);
            let offset_delta = get_zigzag64(buff);

            let key_len = get_zigzag64(buff);
            let key = if key_len <= 0 {
                &buff.bytes()[0..0]
            } else {
                &buff.bytes()[0..key_len as usize]
            };

            let val_len = get_zigzag64(buff);
            let val = if val_len <= 0 {
                &buff.bytes()[0..0]
            } else {
                &buff.bytes()[0..val_len as usize]
            };
            recordset.messages.push(String::from_utf8(val.to_vec()).
                context("Recordset deserialize message value")?);
        }

        debug!("{:?}", recordset);
        Ok(recordset)
    }
}