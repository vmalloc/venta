use crate::message::{Message, MessageDestination, PublishedMessage};
use tokio::sync::mpsc::Sender;

pub enum Publisher {
    Pulsar {
        producer: pulsar::Producer<pulsar::TokioExecutor>,
    },
}

#[derive(Clone)]
pub struct BackgroundPublisher {
    pub(crate) tx: Sender<Message>,
}

impl BackgroundPublisher {
    pub fn publish(&self) -> PublishedMessage {
        PublishedMessage {
            message: Default::default(),
            publisher: MessageDestination::Background(self.clone()),
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
