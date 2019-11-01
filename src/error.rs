use failure::Fail;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(std::io::Error),

    #[fail(display = "No broker available")]
    NoBrokerAvailable,

    #[fail(display = "Logger init{}", _0)]
    Logger(log::SetLoggerError),
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