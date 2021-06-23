use anyhow::{format_err, Error, Result};
use chrono::DateTime;
use chrono::Utc;
use pulsar::TokioExecutor;
use std::collections::HashMap;

use crate::BackgroundProducer;

pub(crate) struct Message {
    data: Vec<u8>,
    timestamp: Option<DateTime<Utc>>,
    properties: HashMap<String, String>,
}

impl Message {
    pub(crate) async fn send(
        &self,
        pulsar_producer: &mut pulsar::Producer<TokioExecutor>,
    ) -> Result<()> {
        let message = pulsar::producer::Message {
            payload: self.data.to_vec(),
            properties: self.properties.clone(),
            event_time: self
                .timestamp
                .or_else(|| Some(Utc::now()))
                .map(|ts| ts.timestamp_millis() as u64),
            ..Default::default()
        };

        pulsar_producer
            .send(message)
            .await
            .map(drop)
            .map_err(anyhow::Error::from)
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
        let timestamp = self.timestamp.or_else(|| Some(Utc::now()));
        let properties = self.properties;
        Ok(Message {
            data,
            timestamp,
            properties,
        })
    }
}

pub struct ProducedMessage {
    pub(crate) message: MessageBuilder,
    pub(crate) producer: BackgroundProducer,
}

impl ProducedMessage {
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

    fn build(self) -> Result<(BackgroundProducer, Message)> {
        let message = self
            .message
            .build()
            .map_err(|s| format_err!("Error constructing message: {}", s))?;
        let producer = self.producer;
        Ok((producer, message))
    }

    pub fn enqueue(self) -> Result<()> {
        let (producer, message) = self.build()?;

        producer.enqueue(message)
    }
}
