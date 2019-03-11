use futures::Future;
use futures::task::Waker;
use futures::Poll;
use std::pin::Pin;

/// Replacement for `impl Future for Either`
pub(crate) enum FuturesUnion2<F1: Future, F2: Future>
{
    F1(F1),
    F2(F2),
}

impl<F1: Future<Output=()>,F2: Future<Output=()>> Future for FuturesUnion2<F1,F2> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, waker: &Waker) -> Poll<Self::Output> {
        let this = unsafe {Pin::get_unchecked_mut(self)};
        match this {
            FuturesUnion2::F1(f) => unsafe{Pin::new_unchecked(f)}.poll(waker),
            FuturesUnion2::F2(f) => unsafe{Pin::new_unchecked(f)}.poll(waker),
        }
    }
}