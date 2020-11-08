use crate::publisher::Publisher;
use anyhow::Result;
use pulsar::{Pulsar, TokioExecutor};
pub enum Source {
    Local,
}

impl Source {
    async fn connect_pulsar(&self) -> Result<Pulsar<TokioExecutor>> {
        let addr = "pulsar://127.0.0.1:6650";
        Ok(Pulsar::builder(addr, TokioExecutor).build().await?)
    }
}
pub enum PubSubType {
    Pulsar,
}

impl Source {
    pub fn pulsar(self) -> ClientSpec {
        ClientSpec {
            source: self,
            pubsub_type: PubSubType::Pulsar,
        }
    }
}

pub struct ClientSpec {
    source: Source,
    pubsub_type: PubSubType,
}

impl ClientSpec {
    pub async fn publisher(self, topic_name: &str, publisher_name: &str) -> Result<Publisher> {
        let client = self.source.connect_pulsar().await?;

        Ok(match self.pubsub_type {
            PubSubType::Pulsar => Publisher::Pulsar {
                producer: client
                    .producer()
                    .with_name(publisher_name)
                    .with_topic(topic_name)
                    .build()
                    .await?,
            },
        })
    }
}
