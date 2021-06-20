use crate::producer::Producer;
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
    pub async fn publisher(self, topic_name: &str, producer_name: &str) -> Result<Producer> {
        match self {
            Broker::Pulsar { client } => Ok(Producer::Pulsar {
                topic_name: topic_name.to_owned(),
                producer_name: producer_name.to_owned(),
                producer: client
                    .producer()
                    .with_name(producer_name)
                    .with_topic(topic_name)
                    .build()
                    .await?,
            }),
        }
    }
}
