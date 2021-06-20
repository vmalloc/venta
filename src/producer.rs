use crate::message::{Message, MessageDestination, PublishedMessage};
use tokio::sync::mpsc::Sender;

pub enum Producer {
    Pulsar {
        producer_name: String,
        topic_name: String,
        producer: pulsar::Producer<pulsar::TokioExecutor>,
    },
}

impl Producer {
    pub(crate) fn producer_name(&self) -> &str {
        match self {
            Producer::Pulsar { producer_name, .. } => producer_name,
        }
    }
    pub(crate) fn topic_name(&self) -> &str {
        match self {
            Producer::Pulsar { topic_name, .. } => topic_name,
        }
    }
}

#[derive(Clone)]
pub struct BackgroundPublisher {
    pub(crate) producer_name: String,
    pub(crate) topic_name: String,
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

impl Producer {
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
        let topic_name = publisher.topic_name().to_owned();
        let producer_name = publisher.producer_name().to_owned();
        #[cfg(feature = "metrics")]
        let producer_name_label = producer_name.clone();
        #[cfg(feature = "metrics")]
        let topic_name_label = topic_name.clone();

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
                } else {
                    #[cfg(feature = "metrics")]
                    crate::metrics::NUM_MSGS_SENT
                        .with_label_values(&[&producer_name_label, &topic_name_label])
                        .inc();
                }
            }
        });
        BackgroundPublisher {
            producer_name,
            topic_name,
            tx,
        }
    }
}
