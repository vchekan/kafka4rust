use futures::channel::mpsc;

/// Convenience structure combining sender and receiver to make bi-directional channel
pub(crate) struct BiChannel<TX,RX> {
    pub tx: mpsc::UnboundedSender<TX>,
    pub rx: mpsc::UnboundedReceiver<RX>
}

impl<TX,RX> BiChannel<TX,RX> {
    /// A -> B
    /// A <- B
    pub fn unbounded() -> (BiChannel<TX,RX>, BiChannel<RX,TX>) {
        let (tx_a, rx_b) = mpsc::unbounded::<TX>();
        let (tx_b, rx_a) = mpsc::unbounded::<RX>();
        let a = BiChannel {tx: tx_a, rx: rx_a};
        let b = BiChannel {tx: tx_b, rx: rx_b};
        (a, b)
    }
}