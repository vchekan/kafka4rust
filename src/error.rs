//! `BrokerError` is error happen during communication with a broker, whereas `InternalError` is
//! caused by driver itself (for example channel communication error).

use thiserror::Error;
use std::io;
use crate::types::BrokerId;
use crate::retry_policy::ShouldRetry;

//pub(crate) type Result<T,E=InternalError> = std::result::Result<T,E>;
/// Result with focus on retry-ability. Higher-level function wrap the response into retry policy
/// and make decision on either underlying connection must be reset.
pub(crate) type BrokerResult<T> = std::result::Result<T,BrokerFailureSource>;

/// User-facing error
#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("No broker available. {0}")]
    NoBrokerAvailable(String),

    //#[error("Dns resolution failed: {0}")]
    //DnsFailed(String),

    #[error("Config error. {0}")]
    Config(String),
}


// TODO: `Cluster` is exposed for admin tools and exposes internal errors.
// Think how to solve it. Create public copy of `Cluster` and re-wrap errors
// into more standardized? But then perhaps some admin functionality won't be translated.
// So, perhaps exposing internal api in admin interface is the only choice?
#[derive(Debug, Error)]
pub enum InternalError {
    #[error("timeout")]
    Timeout,

    #[error("networking operation failed: {context}")]
    Network {context: String, source: Box<dyn std::error::Error + 'static + Send + Sync>},

    #[error("{1}")]
    Io(#[source] std::io::Error, String),

    #[error("{1}")]
    Context(#[source] Box<dyn std::error::Error + 'static + Send + Sync>, String),

    #[error("broker response")]
    BrokerResponseError{#[from] error_code: crate::protocol::ErrorCode},

    #[error("Corrupt message. {0}")]
    CorruptMessage(&'static str),

    #[error("Unexpected recordset magic. Can handle only '2' but got '{0}'")]
    UnexpectedRecordsetMagic(u8),

    /// Upon this error broker connection should be reset and connection reestablished
    #[error("serialization {0}")]
    Serialization(#[source] anyhow::Error),

    #[error("broker failed {0}")]
    BrokerFailure(BrokerFailureSource),

    // TODO: handle critical properly in strategic points
    /// Means to indicate that continuation is not possible due to severity of error.
    /// Driver should exit gracefully.
    #[error("critical: {0}")]
    Critical(String),
}

/// Special error indicating that broker failure requires metadata to be re-fetched because
/// possible rebalancing have happened. The error can be triggered by TCP failures or partition
/// status indicating that given broker is not a leader anymore for the partition.
#[derive(Debug, Error)]
pub enum BrokerFailureSource {
    #[error(transparent)]
    Connect(#[from] io::Error),

    #[error(transparent)]
    KafkaErrorCode(#[from] crate::protocol::ErrorCode),

    #[error("serialization")]
    Serialization(#[source] anyhow::Error),
    
    #[error("timeout")]
    Timeout,
    
    #[error("{0}. {1}")]
    Write(String, io::Error),
    
    #[error("read failed at position {0}. {1}")]
    Read(usize, io::Error),
    
    #[error("unknown broker_id: {0}")]
    UnknownBrokerId(BrokerId),

    // TODO: collect errors for every broker and expose as a collection
    #[error("no broker available")]
    NoBrokerAvailable,
}

impl ShouldRetry for BrokerFailureSource {
    // TODO: Seems line no non-retryable errors, the only one is kafka non-retryable
    fn should_retry(&self) -> bool {
        use BrokerFailureSource::*;
        match self {
            Connect(_) => true,
            KafkaErrorCode(e) => e.is_retriable(),
            // TODO: seems "retryable" is not enough, need to distinct requirement to reset connection
            Serialization(_) => true,
            Timeout => true,
            Write(_,_) => true,
            Read(_,_) => true,
            UnknownBrokerId(_) => true,
            NoBrokerAvailable => true,
        }
    }
}