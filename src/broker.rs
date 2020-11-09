use crate::publisher::Publisher;
use anyhow::Result;
use pulsar::TokioExecutor;
use url::Url;

pub enum Broker {
    Pulsar {
        client: pulsar::Pulsar<TokioExecutor>,
    },
}

pub struct Pulsar;

impl Pulsar {
    pub async fn connect_local() -> Result<Broker> {
        Self::connect(&"pulsar://127.0.0.1:6650".parse()?).await
    }

    pub async fn connect(url: &Url) -> Result<Broker> {
        let client = pulsar::Pulsar::builder(url.to_string(), TokioExecutor)
            .build()
            .await?;
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
