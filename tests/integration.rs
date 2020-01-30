extern crate kafka4rust;
use kafka4rust::Producer;

#[test]
fn topic_is_autocreated_by_producer() {
    let bootstrap = &["broker1"];
    //let producer = Producer::connect(bootstrap).await;
}