use crate::error::BrokerResult;
use std::future::Future;
use tokio::time::Duration;
use tracing_futures::Instrument;
use tracing::trace;

pub(crate) enum RepeatResult<T> {
    Timeout,
    Ok(T)
}

pub(crate) async fn repeat_with_timeout<FF, F, T>(f: FF, delay: Duration, timeout: Duration) -> RepeatResult<F::Output>
    where FF: Fn() -> F,
        F: Future<Output = BrokerResult<T>>
{
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            _ = &mut timeout => {
                tracing::debug!("Operation timed out");
                return RepeatResult::Timeout;
            }
            res = f() => match res {
                Ok(r) => {
                    return {
                        trace!("repeat() succeeded");
                        RepeatResult::Ok(Ok(r))
                    }
                }
                Err(e) => {
                    trace!("Function failed, will retry: {:#}", e);
                    tokio::time::sleep(delay)
                        .instrument(tracing::trace_span!("Retry sleep"))
                        .await;
                }
            }
        }
    }
}

