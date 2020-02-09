use failure::Fail;
use failure::Backtrace;
use failure;
use crate::protocol::ErrorCode;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(std::io::Error),

    #[fail(display = "No broker available")]
    NoBrokerAvailable(Backtrace),

    #[fail(display = "Logger init{}", _0)]
    Logger(log::SetLoggerError),

    #[fail(display = "Spawn error: {}", _0)]
    SpawnError(futures::task::SpawnError),

    #[fail(display = "Dns resolution failed: {}", _0)]
    DnsFailed(String),

    #[fail(display = "Error: {}", _0)]
    ContextStr(failure::Context<&'static str>),

    #[fail(display = "Error: {}", _0)]
    ContextString(failure::Context<String>),

    #[fail(display = "Unexpected recordset magic. Can handle only '2' but got '{}'", _0)]
    UnexpectedRecordsetMagic(u8),

    #[fail(display = "Corrupt message")]
    CorruptMessage,

    #[fail(display = "Kafka error {:?}", _0)]
    KafkaError(Vec<(usize,ErrorCode)>)
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(e: log::SetLoggerError) -> Self {
        Error::Logger(e)
    }
}

impl From<futures::task::SpawnError> for Error {
    fn from(e: futures::task::SpawnError) -> Self { Error::SpawnError(e) }
}

impl From<failure::Context<&str>> for Error {
    fn from(e: failure::Context<&str>) -> Self {
        Error::ContextStr(e)
    }
}

impl From<failure::Context<String>> for Error {
    fn from(e: failure::Context<String>) -> Self {
        Error::ContextString(e)
    }
}
