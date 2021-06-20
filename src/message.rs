use crate::producer::BackgroundPublisher;
use crate::producer::Producer;
use anyhow::{bail, format_err, Error, Result};
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;

pub(crate) struct Message {
    data: Vec<u8>,
    timestamp: Option<DateTime<Utc>>,
    properties: HashMap<String, String>,
}

impl Message {
    pub(crate) async fn send(&self, publisher: &mut Producer) -> Result<()> {
        match publisher {
            Producer::Pulsar { producer, .. } => {
                let message = pulsar::producer::Message {
                    payload: self.data.to_vec(),
                    properties: self.properties.clone(),
                    event_time: self
                        .timestamp
                        .or_else(|| Some(Utc::now()))
                        .map(|ts| ts.timestamp_millis() as u64),
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
    properties: HashMap<String, String>,
    timestamp: Option<DateTime<Utc>>,
}

impl MessageBuilder {
    fn build(self) -> Result<Message> {
        let data = self
            .data
            .ok_or_else(|| format_err!("No data set"))
            .and_then(|data| data)?;
        let timestamp = self.timestamp;
        let properties = self.properties;
        Ok(Message {
            data,
            timestamp,
            properties,
        })
    }
}

pub struct PublishedMessage<'a> {
    pub(crate) message: MessageBuilder,
    pub(crate) publisher: MessageDestination<'a>,
}

pub(super) enum MessageDestination<'a> {
    Sync(&'a mut Producer),
    Background(BackgroundPublisher),
}

impl<'a> MessageDestination<'a> {
    fn producer_name(&self) -> &str {
        match self {
            MessageDestination::Sync(p) => p.producer_name(),
            MessageDestination::Background(bg) => &bg.producer_name,
        }
    }

    fn topic_name(&self) -> &str {
        match self {
            MessageDestination::Sync(p) => p.topic_name(),
            MessageDestination::Background(bg) => &bg.topic_name,
        }
    }
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

    pub fn property(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.message.properties.insert(name.into(), value.into());
        self
    }

    pub fn properties(mut self, properties: impl Iterator<Item = (String, String)>) -> Self {
        for (key, value) in properties {
            self.message.properties.insert(key, value);
        }
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
            MessageDestination::Background(p) => {
                p.tx.send(message)
                    .await
                    .map_err(|_| format_err!("Failed to enqueue message"))
            }
        }
    }

    pub fn enqueue(self) -> Result<()> {
        let (producer, message) = self.build()?;

        if let MessageDestination::Background(p) = &producer {
            let res =
                p.tx.try_send(message)
                    .map_err(|_| format_err!("Cannot enqueue message"));
            #[cfg(feature = "metrics")]
            if res.is_ok() {
                crate::metrics::NUM_MSGS_QUEUED
                    .with_label_values(&[producer.producer_name(), producer.topic_name()])
                    .inc();
            }
            res
        } else {
            bail!("Cannot enqueue when not using a background publisher");
        }
    }
}
