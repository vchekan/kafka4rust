use super::api::*;
use bytes::Buf;
use crate::error::{Result, Error};
use crate::zigzag::get_zigzag64;
use failure::Fail;
use failure::ResultExt;

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
    error_code: ErrorCode,
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
    error_code: ErrorCode,
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
    error_code: ErrorCode,
    topic: String,
    partition_metadata: [PartitionMetadata]
});
response!(PartitionMetadata {
    error_code: ErrorCode,
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
            error_code: ErrorCode,
            offsets: [u64]
        }]
    }]
});

response!(ListGroupResponse0 {
    error_code: ErrorCode,
    groups: [Group {
        group_id: String,
        protocol_type: String
    }]
});

// 18
response!(ApiVersionsResponse0 {
    error_code: ErrorCode,
    api_versions: [ApiVersions]
});

response!(ApiVersions {
    api_key: i16,
    min_version: i16,
    max_version: i16
});

response!(ApiVersionsResponse1 {
    error_code: ErrorCode,
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

#[repr(i16)]
#[derive(Debug)]
pub enum ErrorCode {
    UnknownServerError = -1,
    NONE = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    DelegationTokenAuthDisabled = 61,
    DelegationTokenNotFound = 62,
    DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64,
    DelegationTokenAuthorizationFailed = 65,
    DelegationTokenExpired = 66,
    InvalidPrincipalType = 67,
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    FetchSessionIdNotFound = 70,
    InvalidFetchSessionEpoch = 71,
    ListenerNotFound = 72,
    TopicDeletionDisabled = 73,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompressionType = 76,
    StaleBrokerEpoch = 77,
    OffsetNotAvailable = 78,
    MemberIdRequired = 79,
    PreferredLeaderNotAvailable = 80,
    GroupMaxSizeReached = 81,
    FencedInstanceId = 82,
    EligibleLeadersNotAvailable = 83,
    ElectionNotNeeded = 84,
    NoReassignmentInProgress = 85,
    GroupSubscribedToTopic = 86,
    InvalidRecord = 87,
}