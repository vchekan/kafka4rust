use tokio::time::Duration;
use std::future::Future;
use anyhow::Result;
use tracing_futures::Instrument;

pub async fn repeat<FF,F,T>(f: FF, delay: Duration, mut retries: u32) -> F::Output
    where FF: Fn() -> F,
          F: Future<Output=Result<T>>
{
    loop {
        match f().await {
            Ok(r) => return {
                trace!("repeat() succeeded");
                Ok(r)
            },
            Err(e) => {
                if retries == 0 {
                    return Err(e);
                }
                retries -= 0;
                trace!("Function failed, will retry: {:#}", e);
                tokio::time::sleep(delay).instrument(tracing::trace_span!("Retry sleep")).await;
            }
        }
    }
}