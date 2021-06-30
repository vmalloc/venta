use crate::message::{Message, ProducedMessage};
use anyhow::format_err;
use anyhow::Result;
use futures::Future;
use pulsar::Producer;
use pulsar::TokioExecutor;
use std::error::Error as StdError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, Sender};

const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct BackgroundProducer {
    topic_name: String,
    tx: Sender<Message>,
    pending: Arc<AtomicU64>,
}

struct RetryQueue {
    rx: Receiver<Message>,
    unsent: Option<Message>,
    next_retry: Option<Instant>,
}

impl RetryQueue {
    async fn next(&mut self) -> Option<Message> {
        if let Some(ts) = self.next_retry.take() {
            tokio::time::sleep_until(ts.into()).await;
        }
        if let Some(message) = self.unsent.take() {
            Some(message)
        } else {
            self.rx.recv().await
        }
    }

    fn schedule_retry(&mut self, message: Message) {
        assert!(
            self.unsent.is_none(),
            "schedule_retry called with an already pending message"
        );
        self.unsent.replace(message);
        self.next_retry = Some(Instant::now() + RETRY_DELAY);
    }
}

impl BackgroundProducer {
    pub async fn spawn_simple(
        url: impl Into<String>,
        topic: impl Into<String>,
        producer_name: Option<String>,
    ) -> Result<Self> {
        let url: String = url.into();
        let topic: String = topic.into();

        Self::spawn(move || {
            let url = url.clone();
            let topic = topic.clone();
            let producer_name = producer_name.clone();
            async move {
                let mut returned = pulsar::Pulsar::builder(url.clone(), TokioExecutor)
                    .build()
                    .await?
                    .producer()
                    .with_topic(topic);
                if let Some(producer_name) = producer_name.clone() {
                    returned = returned.with_name(&producer_name);
                }

                returned.build().await
            }
        })
        .await
    }

    pub async fn spawn<Fut, F, E>(producer_factory: F) -> Result<Self>
    where
        Fut: Future<Output = Result<Producer<TokioExecutor>, E>> + Send,
        E: Into<anyhow::Error> + StdError,
        F: Fn() -> Fut + Send + Sync + 'static,
    {
        let mut producer = Some(producer_factory().await.map_err(Into::into)?);
        let topic_name = producer.as_ref().unwrap().topic().to_owned();

        let (tx, rx) = tokio::sync::mpsc::channel::<Message>(1000);
        let pending = Arc::new(AtomicU64::new(0));
        let pending_msgs = pending.clone();
        let mut queue = RetryQueue {
            rx,
            unsent: None,
            next_retry: None,
        };
        #[cfg(feature = "metrics")]
        let topic_name_label = topic_name.clone();

        tokio::task::spawn(async move {
            loop {
                let message = match queue.next().await {
                    None => break,
                    Some(message) => message,
                };

                if producer.is_none() {
                    match producer_factory().await {
                        Ok(p) => producer = Some(p),
                        Err(e) => {
                            log::error!("Failed recreting producer: {:?}", e);
                            queue.schedule_retry(message);
                            continue;
                        }
                    }
                }

                let res =
                    tokio::time::timeout(SEND_TIMEOUT, message.send(producer.as_mut().unwrap()))
                        .await;

                let needs_producer_recreate = res.is_err();

                if let Err(e) = res
                    .map_err(|elapsed| format_err!("Timeout sending message after {:?}", elapsed))
                    .and_then(|r| r.map_err(anyhow::Error::from))
                {
                    log::error!("Sending message failed: {:?}", e);
                    queue.schedule_retry(message);
                    if needs_producer_recreate {
                        producer = None
                    }
                } else {
                    pending_msgs.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    #[cfg(feature = "metrics")]
                    crate::metrics::NUM_MSGS_SENT
                        .with_label_values(&[&topic_name_label])
                        .inc();
                }
            }
        });
        Ok(Self {
            topic_name,
            tx,
            pending,
        })
    }

    pub fn produce(&self) -> ProducedMessage {
        ProducedMessage {
            message: Default::default(),
            producer: self.clone(),
        }
    }

    pub fn has_pending_messages(&self) -> bool {
        self.pending.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn enqueue(&self, msg: Message) -> Result<()> {
        self.tx
            .try_send(msg)
            .map_err(|_| format_err!("Cannot enqueue message"))
            .map(|()| {
                self.pending
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                #[cfg(feature = "metrics")]
                crate::metrics::NUM_MSGS_QUEUED
                    .with_label_values(&[&self.topic_name])
                    .inc();
            })
    }
}
