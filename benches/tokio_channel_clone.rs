use criterion::{criterion_group, criterion_main, Criterion};
use kafka4rust::Response;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

async fn direct_send(rx: &mut Receiver<Response>, tx: &mut Sender<Response>) {
    let res = Response::Ack {
        partition: 1,
        offset: 2,
        error: kafka4rust::protocol::ErrorCode::None,
    };
    tx.send(res).await.unwrap();
    let _ = rx.recv().await.unwrap();
}

async fn cloned_send(rx: &mut Receiver<Response>, tx: &mut Sender<Response>) {
    let mut tx2 = tx.clone();
    let res = Response::Ack {
        partition: 1,
        offset: 2,
        error: kafka4rust::protocol::ErrorCode::None,
    };
    tx2.send(res).await.unwrap();
    let _ = rx.recv().await.unwrap();
}

fn bench_tokio_clone(c: &mut Criterion) {
    let mut rt = Runtime::new().unwrap();
    let (mut tx, mut rx) = channel::<Response>(1000);
    let mut group = c.benchmark_group("Tokio channel clone");
    group.bench_function("Direct", |b| {
        b.iter(|| rt.block_on(direct_send(&mut rx, &mut tx)))
    });
    group.bench_function("Cloned", |b| {
        b.iter(|| rt.block_on(cloned_send(&mut rx, &mut tx)))
    });
    group.finish();
}

criterion_group!(benches, bench_tokio_clone);
criterion_main!(benches);
