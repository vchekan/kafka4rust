use futures::{StreamExt, channel::mpsc};

/// Bi-directional channel built with 2 unbounded channels
/// TODO: implement async pushback with semaphore?
pub fn bichannel<IN,OUT,F>(handle: F) -> (mpsc::UnboundedSender<IN>, mpsc::UnboundedReceiver<OUT>)
    where
        OUT: Send + 'static,
        IN: Send + 'static,
        F: Fn(IN) -> OUT + Send + Sync + 'static
{
    let (tx, rx) = mpsc::unbounded();
    let (result_tx, result_rx) = mpsc::unbounded();
    juliex::spawn(async move {
        debug!("Loop started");
        rx.for_each(|msg| {
            let response = handle(msg);
            result_tx.unbounded_send(response).unwrap();
            futures::future::ready(())
        }).await;
        debug!("Loop finished");
    });

    (tx, result_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor;
    use simplelog::*;

    #[test]
    fn component() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Stdout).unwrap()
        ]).unwrap();

        executor::block_on(async {
            let (tx,mut rx) = bichannel(|msg| {
                debug!("Received: {}", msg);
                "Pong".to_string()
            });

            debug!("Sending ping");
            tx.unbounded_send("Ping".to_string()).unwrap();
            let res = rx.select_next_some().await;
            debug!("Response: {}", res);
            tx.close_channel();
            debug!("Closed channel");
        });
    }
}