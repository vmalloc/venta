# Overview

Venta is a high-level ergonomic wrapper around pub-sub services, aimed at being efficient, idiomatic and convenient.

# Usage

To use against a local broker (in this case a Pulsar broker):

```rust
    async fn f() -> anyhow::Result<()> {
        let mut publisher = venta::Pulsar::connect_local()
            .await?
            .publisher("some_topic", "producer_name")
            .await?;

        publisher.publish().text("something").send().await
    }
```

Venta can either work in "foreground" mode or in "background" mode. When in foreground (which is the default), the `send` operation awaits the underlying sending to the broker. 

While waiting for the send operation to complete is useful in some cases, in other cases you want to get back to the main flow of your program and let the sending happen in the background. This handles, among else, long delays when reconnecting to the broker, and more. Running in the background is simple:

```rust

    async fn f() -> anyhow::Result<()> {
        let bg_publisher = venta::Pulsar::connect_local()
            .await?
            .publisher("topic", "producer_name")
            .await?
            .background();

        let json_data = serde_json::json!({"field": 2});
    
        bg_publisher.publish().json(&json_data).enqueue()
    }
```
Note that `enqueue` is not async - it immediately tries to send the message to an internal queue (which will fail if it is full), and has the background publisher publish the message on its own time. 

`bg_publisher` is `Clone`, so it can be passed around to multiple locations in your code to have them publish events independently.

