use thiserror::Error;

/// User-facing error
#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("No broker available. {0}")]
    NoBrokerAvailable(String),

    //#[error("Dns resolution failed: {0}")]
    //DnsFailed(String),

    #[error("Unexpected recordset magic. Can handle only '2' but got '{0}'")]
    UnexpectedRecordsetMagic(u8),

    #[error("Corrupt message. {0}")]
    CorruptMessage(&'static str),

    #[error("Config error. {0}")]
    Config(String),
}
