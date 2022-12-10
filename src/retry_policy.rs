use std::time::Duration;
use std::result::Result;
use std::future::Future;
use log::debug;
use tokio::time::timeout as tokio_timeout;
use crate::error::{BrokerFailureSource, BrokerResult};

pub async fn with_retry<FF,F,R/*,E*/>(delay: Duration, timeout: Duration, f: FF) -> BrokerResult<R> //Result<R,E>
    where
        FF: Fn() -> F,
        FF::Output: Future<Output=Result<R,/*E*/BrokerFailureSource>>,
        F: Future<Output=Result<R,/*E*/BrokerFailureSource>>,
        // E: ShouldRetry + std::fmt::Debug,
{
    loop {
        match tokio_timeout(timeout, f()).await {
            Ok(res) => match res {
                Ok(res) => {
                    debug!("with_retry: Ok");
                    return Ok(res)
                },
                Err(e) => match e.should_retry() {
                    true => {
                        debug!("Error, will retry: {:?} in {:?}", e, delay);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    false => return Err(e),
                }
            }
            Err(_) => return Err(BrokerFailureSource::Timeout)
        }
    }
}

/// Evaluate either given error should be retried or is a terminal one
pub trait ShouldRetry {
    fn should_retry(&self) -> bool;
}
