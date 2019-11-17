use failure::Fail;
use failure::Backtrace;

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
    DnsFailed(String)
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