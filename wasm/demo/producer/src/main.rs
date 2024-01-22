use fake::faker::name::raw::Name;
use fake::locales::EN;
use fake::Fake;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, Deserialize)]
struct Message {
    id: u32,
    name: String,
}

#[tokio::main]
async fn main() {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut count = 1;

    loop {
        sleep(Duration::from_secs(1)).await;
        count += 1;

        let message = Message {
            id: count,
            name: format!("{}", Name(EN).fake::<String>()),
        };

        let payload = serde_json::to_string(&message).unwrap();
        println!("{}", payload.to_string());

        let record = FutureRecord::to("topic")
            .partition(0)
            .key("")
            .payload(payload.as_str());
        producer
            .send(record, Duration::from_secs(1))
            .await
            .expect("Failed to send record");
    }
}
