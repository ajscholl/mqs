extern crate chrono;
extern crate hyper;

use mqs::client::Service;
use mqs::routes::queues::QueueConfig;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::env;
use chrono::Utc;
use std::ops::Sub;
use hyper::HeaderMap;
use hyper::header::{HeaderValue, CONTENT_TYPE, CONTENT_ENCODING};
use tokio::runtime::Builder;

type AnyError = Box<dyn std::error::Error + Send + Sync>;

const NUM_THREADS: usize = 25;

#[derive(Debug)]
struct StringError {
    error: &'static str,
}

impl StringError {
    fn new(error: &'static str) -> StringError {
        StringError { error }
    }
}

impl Display for StringError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl Error for StringError {}

fn get_service() -> Service {
    let host = env::var("MQS_SERVER").unwrap_or("localhost".to_string());
    Service::new(&format!("http://{}:7843", &host))
}

fn main() -> Result<(), AnyError> {
    let mut rt = Builder::new()
        .enable_all()
        .threaded_scheduler()
        .core_threads(NUM_THREADS)
        .build()
        .unwrap();

    rt.block_on(async {
        let s = get_service();
        let queue_count: usize = 10;
        let queues = s.get_queues(None, None).await?;
        // clear data
        for queue in queues.queues {
            let result = s.delete_queue(&queue.name).await?;

            if result.is_none() {
                Err(StringError::new("Failed to delete queue"))?;
            }
        }

        // create some test queues
        for i in 0..queue_count {
            let result = s.create_queue(&format!("test-queue-{}", i), &QueueConfig {
                redrive_policy: None,
                retention_timeout: 3600,
                visibility_timeout: 5,
                message_delay: 0,
                message_deduplication: false,
            }).await?;

            if result.is_none() {
                Err(StringError::new("Failed to create queue"))?;
            }
        }

        // update test queues
        for i in 0..queue_count {
            let result = s.update_queue(&format!("test-queue-{}", i), &QueueConfig {
                redrive_policy: None,
                retention_timeout: 3600,
                visibility_timeout: 10,
                message_delay: 0,
                message_deduplication: false,
            }).await?;

            if result.is_none() {
                Err(StringError::new("Failed to update queue"))?;
            }
        }

        let start_publish = Utc::now();
        let mut pending = Vec::with_capacity(queue_count);

        for i in 0..queue_count {
            let queue = format!("test-queue-{}", i);
            let queue2 = queue.clone();
            let handle = tokio::spawn(async move {
                publish_messages(i, queue2).await.unwrap()
            });
            pending.push((queue, handle));
        }

        for (queue, work) in pending {
            work.await?;
            let info = s.describe_queue(&queue).await?;
            if let Some(description) = info {
                if description.status.messages != 10000 {
                    Err(StringError::new("Wrong number of messages found"))?;
                }
            } else {
                Err(StringError::new("Failed to describe queue"))?;
            }
        }

        let start_consume = Utc::now();
        let publish_took = start_consume.clone().sub(start_publish);
        println!("Publishing took: {}", publish_took);
        let mut pending = Vec::with_capacity(queue_count);

        for i in 0..queue_count {
            let queue = format!("test-queue-{}", i);
            let queue2 = queue.clone();
            let handle = tokio::spawn(async move {
                consume_messages(i, queue2).await.unwrap()
            });
            pending.push((queue, handle));
        }

        for (queue, work) in pending {
            work.await?;
            let info = s.describe_queue(&queue).await?;
            if let Some(description) = info {
                if description.status.messages != 0 {
                    Err(StringError::new("Queue not yet empty"))?;
                }
            } else {
                Err(StringError::new("Failed to describe queue"))?;
            }
        }

        let end_consume = Utc::now();
        let consume_took = end_consume.sub(start_consume);
        println!("Consuming took: {}", consume_took);

        Ok(())
    })
}

const DEFAULT_MESSAGE: [&'static [u8]; 3] = [
    "{\"data\":\"a test\"}".as_bytes(),
    &[6, 3, 6, 8, 0, 0, 0, 1, 5, 22, 254, 0, 32, 100, 128, 0],
    &[1, 2, 5, 3, 7, 4, 98, 66, 127, 0, 255, 192],
];
const DEFAULT_MESSAGE_CONTENT_TYPE: [&'static str; 3] = [
    "application/json",
    "text/html",
    "image/png",
];
const DEFAULT_MESSAGE_CONTENT_ENCODING: [Option<&'static str>; 3] = [
    Some("identity"),
    Some("gzip"),
    None,
];

async fn publish_messages(index: usize, queue: String) -> Result<(), AnyError> {
    let s = get_service();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(DEFAULT_MESSAGE_CONTENT_TYPE[index % DEFAULT_MESSAGE_CONTENT_TYPE.len()]));
    if let Some(value) = DEFAULT_MESSAGE_CONTENT_ENCODING[index % DEFAULT_MESSAGE_CONTENT_ENCODING.len()] {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static(value));
    }
    let message = DEFAULT_MESSAGE[index % DEFAULT_MESSAGE.len()].to_owned();
    let mut message_bundle = Vec::with_capacity(if index > 5 { 10 } else { 1 });
    for _ in 0..message_bundle.capacity() {
        message_bundle.push((headers.clone(), message.clone()));
    }
    for _ in 0..10000 / message_bundle.len() {
        let result = s.publish_messages(&queue, &message_bundle).await?;
        if !result {
            Err(StringError::new("Expected successful publish"))?;
        }
    }

    Ok(())
}

async fn consume_messages(index: usize, queue: String) -> Result<(), AnyError> {
    let mut handles = Vec::with_capacity(NUM_THREADS);
    for _ in 0..handles.capacity() {
        let queue_name = queue.clone();
        let handle = tokio::spawn(async move {
            let s = get_service();
            loop {
                let messages = s.get_messages(&queue_name, 10).await?;
                if messages.is_empty() {
                    break;
                }
                for message in messages {
                    if &message.content_type != DEFAULT_MESSAGE_CONTENT_TYPE[index % DEFAULT_MESSAGE_CONTENT_TYPE.len()] {
                        Err(StringError::new("Message content type does not match"))?;
                    }
                    match DEFAULT_MESSAGE_CONTENT_ENCODING[index % DEFAULT_MESSAGE_CONTENT_ENCODING.len()] {
                        None => {
                            if message.content_encoding.is_some() {
                                Err(StringError::new("Unexpected message content encoding"))?;
                            }
                        },
                        Some(encoding) => {
                            if message.content_encoding.is_none() {
                                Err(StringError::new("Message content encoding missing"))?;
                            }
                            if message.content_encoding.unwrap().as_str() != encoding {
                                Err(StringError::new("Message content encoding does not match"))?;
                            }
                        },
                    }
                    if message.content.as_slice() != DEFAULT_MESSAGE[index % DEFAULT_MESSAGE.len()] {
                        Err(StringError::new("Message content does not match"))?;
                    }
                    let deleted = s.delete_message(&message.message_id).await?;

                    if !deleted {
                        Err(StringError::new("Failed to delete message"))?;
                    }
                }
            }

            Ok::<(), AnyError>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
