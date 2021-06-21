use pulsar::TokioExecutor;
use std::time::Duration;
use venta::BackgroundProducer;

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let producer = BackgroundProducer::spawn(|| async move {
        pulsar::Pulsar::builder("pulsar://127.0.0.1", TokioExecutor)
            .build()
            .await?
            .producer()
            .with_topic("test")
            .build()
            .await
    })
    .await
    .expect("Failed to initialize");

    for iteration in 0.. {
        producer
            .produce()
            .json(&serde_json::json!({
                "hello": "there",
                "index": iteration
            }))
            .enqueue()
            .expect("Failed to enqueue");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
