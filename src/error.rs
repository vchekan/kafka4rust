use thiserror::Error;
use std::io;
use crate::types::BrokerId;

pub(crate) type Result<T,E=InternalError> = std::result::Result<T,E>;

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
    BrokerFailure(BrokerFailureSource)
}

/// Special error indicating that broker failure requires metadata to be re-fetched because
/// possible rebalancing have happened. The error can be triggered by TCP failures or partition
/// status indicating that given broker is not a leader anymore for the partition.
#[derive(Debug, Error)]
pub enum BrokerFailureSource {
    #[error(transparent)]
    Connect(#[from] io::Error),
    #[error("timeout")]
    Timeout,
    #[error("{0}. {1}")]
    Write(String, io::Error),
    #[error("read failed at position {0}. {1}")]
    Read(usize, io::Error),
    #[error("unknown broker_id: {0}")]
    UnknownBrokerId(BrokerId),

    // TODO: collect errors for evary broker and expose as a collection
    #[error("no broker available")]
    NoBrokerAvailable,
}

// pub(crate) struct BrokerFailedError(BrokerId, BrokerFailureSource);