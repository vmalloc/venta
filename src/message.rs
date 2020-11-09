use crate::publisher::BackgroundPublisher;
use crate::publisher::Publisher;
use anyhow::{bail, format_err, Error, Result};
use chrono::DateTime;
use chrono::Utc;

pub(crate) struct Message {
    data: Vec<u8>,
    timestamp: Option<DateTime<Utc>>,
}

impl Message {
    pub(crate) async fn send(&self, publisher: &mut Publisher) -> Result<()> {
        match publisher {
            Publisher::Pulsar { producer } => {
                let message = pulsar::producer::Message {
                    payload: self.data.to_vec(),
                    event_time: self.timestamp.map(|ts| ts.timestamp_millis() as u64),
                    ..Default::default()
                };

                producer
                    .send(message)
                    .await
                    .map(drop)
                    .map_err(anyhow::Error::from)
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct MessageBuilder {
    data: Option<Result<Vec<u8>>>,
    timestamp: Option<DateTime<Utc>>,
}

impl MessageBuilder {
    fn build(self) -> Result<Message> {
        let data = self
            .data
            .ok_or_else(|| format_err!("No data set"))
            .and_then(|data| data)?;
        let timestamp = self.timestamp;
        Ok(Message { data, timestamp })
    }
}

pub struct PublishedMessage<'a> {
    pub(crate) message: MessageBuilder,
    pub(crate) publisher: MessageDestination<'a>,
}

pub(super) enum MessageDestination<'a> {
    Sync(&'a mut Publisher),
    Background(BackgroundPublisher),
}

impl<'a> PublishedMessage<'a> {
    pub fn text(mut self, text: impl Into<Vec<u8>>) -> Self {
        self.message.data.replace(Ok(text.into()));
        self
    }

    pub fn json(mut self, json: &impl serde::Serialize) -> Self {
        self.message
            .data
            .replace(serde_json::to_vec(json).map_err(Error::from));
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.message.timestamp.replace(ts);
        self
    }

    fn build(self) -> Result<(MessageDestination<'a>, Message)> {
        let message = self
            .message
            .build()
            .map_err(|s| format_err!("Error constructing message: {}", s))?;
        let publisher = self.publisher;
        Ok((publisher, message))
    }

    pub async fn send(self) -> Result<()> {
        let (publisher, message) = self.build()?;

        match publisher {
            MessageDestination::Sync(p) => message.send(p).await,
            MessageDestination::Background(mut p) => {
                p.tx.send(message)
                    .await
                    .map_err(|_| format_err!("Failed to enqueue message"))
            }
        }
    }

    pub fn enqueue(self) -> Result<()> {
        let (publisher, message) = self.build()?;

        if let MessageDestination::Background(mut p) = publisher {
            p.tx.try_send(message)
                .map_err(|_| format_err!("Cannot enqueue message"))
        } else {
            bail!("Cannot enqueue when not using a background publisher");
        }
    }
}