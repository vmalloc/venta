# Overview

Venta is a high-level ergonomic wrapper around pub-sub services, aimed at being efficient, idiomatic and convenient.

# Usage
```rust
    async fn f() -> anyhow::Result<()> {
        let producer = venta::BackgroundProducer::spawn(|| async {
            pulsar::Pulsar::builder("pulsar://127.0.0.1", pulsar::TokioExecutor)
              .build()
              .await?
              .producer()
              .with_topic("topic").build()
              .await
        }).await?;

        let json_data = serde_json::json!({"field": 2});
    
        producer.produce().json(&json_data).enqueue()
    }
```

For simple cases:
```rust
    async fn f() -> anyhow::Result<()> {
        let producer = venta::BackgroundProducer::spawn_simple("pulsar://127.0.0.1", "topic_name", Some("producer_name".into())).await?;
        let json_data = serde_json::json!({"field": 2});
    
        producer.produce().json(&json_data).enqueue()
    }
```



Note that `enqueue` is not async - it immediately tries to send the message to an internal queue (which will fail if it is full), and has the background publisher publish the message on its own time. 

`bg_publisher` is `Clone`, so it can be passed around to multiple locations in your code to have them publish events independently.

