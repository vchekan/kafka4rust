use anyhow::Result;
use std::future::Future;
use tokio::time::Duration;
use tracing_futures::Instrument;
use tracing_attributes::instrument;
use tokio;

pub async fn repeat_with_timeout<FF, F, T>(f: FF, delay: Duration, timeout: Duration) -> F::Output
    where FF: Fn() -> F,
        F: Future<Output = Result<T>>,
{
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            _ = &mut timeout => {
                tracing::debug!("Operation timed out");
                return Err(anyhow::anyhow!("Timeout"));
            }
            res = f() => match res {
                Ok(r) => {
                    return {
                        trace!("repeat() succeeded");
                        Ok(r)
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
        // match f().await {
        //     Ok(r) => {
        //         return {
        //             trace!("repeat() succeeded");
        //             Ok(r)
        //         }
        //     }
        //     Err(e) => {
        //         if retries == 0 {
        //             return Err(e);
        //         }
        //         retries -= 0;
        //         trace!("Function failed, will retry: {:#}", e);
        //         tokio::time::sleep(delay)
        //             .instrument(tracing::trace_span!("Retry sleep"))
        //             .await;
        //     }
        // }
    }
}

// #[instrument(level="debug", err, skip(f))]
// pub async fn repeat<FF, F, T>(f: FF, delay: Duration, mut retries: u32) -> F::Output
//     where
//         FF: Fn() -> F,
//         F: Future<Output = Result<T>>,
// {
//     // let mut retries = retries;
//     loop {
//         match f().await {
//             Ok(r) => {
//                 return {
//                     trace!("repeat() succeeded");
//                     Ok(r)
//                 }
//             }
//             Err(e) => {
//                 if retries <= 0 {
//                     debug!("Function failed. Retry limit exceeded. {:#?}", e);
//                     return Err(e);
//                 }
//                 retries -= 1;
//                 debug!("Function failed, will retry {} times: {:#}", retries, e);
//                 tokio::time::sleep(delay)
//                     .instrument(tracing::trace_span!("Retry sleep"))
//                     .await;
//             }
//         }
//     }
// }
