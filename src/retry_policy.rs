use std::time::Duration;
use std::result::Result;
use std::future::Future;
use log::debug;
use std::task::{Context, Poll};
use std::pin::Pin;

mod try_1 {
    use super::*;

    pub struct WithRetry {}

    pub enum RetryResult {
        Retry,
        Terminate,
    }

    /// Evaluate either given error should be retried or is a terminal one
    pub trait ErrorEvaluator {
        fn eval(&self) -> RetryResult;
    }


    pub trait WithRetryFn<F: Future> {
        fn with_retry(self, delay: Duration) -> Box<dyn Future<Output=F::Output>>;
    }

    impl<FUT, FUT_FACTORY, RES, ERR> WithRetryFn<FUT> for FUT_FACTORY
        where
            FUT_FACTORY: Fn() -> FUT,
            FUT: Future<Output=Result<RES, ERR>>,
            ERR: ErrorEvaluator
    {
        fn with_retry(self, delay: Duration) -> Box<dyn Future<Output=FUT::Output>> {
            Box::new(async move {
                loop {
                    let future: FUT = self();
                    let res = future.await;
                    match res {
                        Ok(res) => break Ok(res),
                        Err(e) => {
                            match e.eval() {
                                RetryResult::Retry => {
                                    debug!("Retry policy: will try again in {:?}", delay);
                                    tokio::time::sleep(delay).await;
                                    continue;
                                }
                                RetryResult::Terminate => {
                                    debug!("Failed in retry");
                                    break Err(e)
                                }
                            }
                        }
                    }
                }
            })
        }
    }
}

mod try_pin {
    use super::*;

    pub struct Retry<F> {
        f: F
    }

    trait WithRetry {
        type Output;
        fn with_retry(self) -> Retry<Self::Output>;
    }

    impl WithRetry for T where
        T: Fn() -> F,
        F: Future
    {
        type Output = F::Output;

        fn with_retry(self) -> Retry<Self::Output> {
            todo!()
        }
    }
}