# Overview

Venta is a Rust library implementing a robust, ergonomic and non-blocking producer for Apache Pulsar. It builds upon [pulsar-rs](https://github.com/wyyerd/pulsar-rs), but adds some missing pieces:

1. Venta publishes messages in the background, meaning that enqueuing a message happens immediately, given enough queue space. This is useful for applications that do not want to block on the actual publishing operation
2. Venta adds retries and timeouts on top of pulsar-rs, allowing it to recover from errors which cause pulsar-rs to get stuck or return with errors
3. Venta is more ergonomic for the common use cases (e.g. publishing json messages, adding properties etc.)
# Usage

For a simple use case, in which you would like to configure a producer with a topic name and a producer name, you can use `spawn_simple`:

```rust
    async fn f() -> anyhow::Result<()> {
        let producer = venta::BackgroundProducer::spawn_simple("pulsar://127.0.0.1", "topic_name", Some("producer_name".into())).await?;
        //...
        Ok(())
    }
```

For cases in which you would like more fine-grained control over how the producer is built, you can use the `spawn` constructor, receiving a closure for creating the producer:

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
        //...
        Ok(())
    }
```

See [the documentation for pulsar-rs](https://github.com/wyyerd/pulsar-rs) for more information on how to contstruct the underlying client and producer.



Once a producer is initialized, enqueueing json messages is relatively simple


```rust
    use serde_json::json;
    
    async fn f() -> anyhow::Result<()> {
        let producer = venta::BackgroundProducer::spawn_simple("pulsar://127.0.0.1", "topic_name", Some("producer_name".into())).await?;

        producer.produce().json(&json!({
            "message": "here"
        })).enqueue()
    }
```

Venta producers are `Clone`, meaning they can be passed around to various parts of your code without any issues.