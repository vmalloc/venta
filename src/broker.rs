use crate::publisher::Publisher;
use anyhow::Result;
use pulsar::TokioExecutor;

pub enum Broker {
    Pulsar {
        client: pulsar::Pulsar<TokioExecutor>,
    },
}

pub struct Pulsar;

impl Pulsar {
    pub async fn connect_local() -> Result<Broker> {
        let addr = "pulsar://127.0.0.1:6650";
        let client = pulsar::Pulsar::builder(addr, TokioExecutor).build().await?;
        Ok(Broker::Pulsar { client })
    }
}

impl Broker {
    pub async fn publisher(self, topic_name: &str, publisher_name: &str) -> Result<Publisher> {
        match self {
            Broker::Pulsar { client } => Ok(Publisher::Pulsar {
                producer: client
                    .producer()
                    .with_name(publisher_name)
                    .with_topic(topic_name)
                    .build()
                    .await?,
            }),
        }
    }
}
