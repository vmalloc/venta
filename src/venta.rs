use anyhow::{bail, format_err, Error, Result};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;

pub enum Publisher {
    Pulsar {
        producer: pulsar::Producer<pulsar::TokioExecutor>,
    },
}

#[derive(Clone)]
pub struct BackgroundPublisher {
    tx: Sender<Message>,
}

impl BackgroundPublisher {
    pub fn publish(&self) -> PublishedMessage {
        PublishedMessage {
            message: Default::default(),
            publisher: MessageDestination::Background(self.clone()),
        }
    }
}

struct Message {
    data: Vec<u8>,
    timestamp: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct MessageBuilder {
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
    message: MessageBuilder,
    publisher: MessageDestination<'a>,
}

enum MessageDestination<'a> {
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

impl Message {
    pub async fn send(&self, publisher: &mut Publisher) -> Result<()> {
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

impl Publisher {
    pub fn publish(&mut self) -> PublishedMessage<'_> {
        PublishedMessage {
            message: Default::default(),
            publisher: MessageDestination::Sync(self),
        }
    }

    pub fn background(self) -> BackgroundPublisher {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(1000);
        let mut publisher = self;
        let mut unsent = None;

        tokio::task::spawn(async move {
            loop {
                let message = if let Some(message) = unsent.take() {
                    message
                } else {
                    rx.recv().await
                };
                if message.is_none() {
                    break;
                }
                let message = message.unwrap();
                if let Err(e) = message.send(&mut publisher).await {
                    log::error!("Error sending message: {:?}", e);
                    unsent.replace(Some(message));
                }
            }
        });
        BackgroundPublisher { tx }
    }
}
