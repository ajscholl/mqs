#![warn(
    missing_docs,
    rust_2018_idioms,
    future_incompatible,
    missing_copy_implementations,
    trivial_numeric_casts,
    unsafe_code,
    unused,
    unused_qualifications,
    variant_size_differences
)]
#![cfg_attr(test, deny(warnings))]

//! Benchmark application which creates a lot of messages and consumes them again against a MQS server.
//! Used in CI tests.

use chrono::Utc;
use std::{
    env,
    error::Error,
    fmt::{Display, Formatter},
    ops::Sub,
};
use tokio::runtime::Builder;
use uuid::Uuid;

use mqs_client::{ClientError, PublishableMessage, Service};
use mqs_common::QueueConfig;

type AnyError = Box<dyn Error + Send + Sync>;

const NUM_THREADS: usize = 25;

#[derive(Debug)]
struct StringError {
    error: &'static str,
}

impl StringError {
    const fn new(error: &'static str) -> StringError {
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
        {
            match get_service()
                .set_max_body_size(Some(1))
                .get_queues(None, None, None)
                .await
            {
                Err(ClientError::TooLargeResponse) => {},
                _ => {
                    Err(StringError::new("request should have been aborted"))?;
                },
            }
        }
        let s = get_service();
        let queue_count: usize = 10;
        let queues = s.get_queues(None, None, None).await?;
        // clear data
        for queue in queues.queues {
            let result = s.delete_queue(&queue.name, None).await?;

            if result.is_none() {
                Err(StringError::new("Failed to delete queue"))?;
            }
        }

        // create some test queues
        for i in 0..queue_count {
            let result = s
                .create_queue(&format!("test-queue-{}", i), None, &QueueConfig {
                    redrive_policy:        None,
                    retention_timeout:     3600,
                    visibility_timeout:    100,
                    message_delay:         0,
                    message_deduplication: false,
                })
                .await?;

            if result.is_none() {
                Err(StringError::new("Failed to create queue"))?;
            }
        }

        // update test queues
        for i in 0..queue_count {
            let result = s
                .update_queue(&format!("test-queue-{}", i), None, &QueueConfig {
                    redrive_policy:        None,
                    retention_timeout:     3600,
                    visibility_timeout:    300,
                    message_delay:         0,
                    message_deduplication: false,
                })
                .await?;

            if result.is_none() {
                Err(StringError::new("Failed to update queue"))?;
            }
        }

        let start_publish = Utc::now();
        let mut pending = Vec::with_capacity(queue_count);

        for i in 0..queue_count {
            let queue = format!("test-queue-{}", i);
            let queue2 = queue.clone();
            let handle = tokio::spawn(async move { publish_messages(i, queue2).await.unwrap() });
            pending.push((queue, handle));
        }

        for (queue, work) in pending {
            work.await?;
            let info = s.describe_queue(&queue, None).await?;
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
            let handle = tokio::spawn(async move { consume_messages(i, queue2, None).await.unwrap() });
            pending.push((queue, handle));
        }

        for (queue, work) in pending {
            work.await?;
            check_queue_empty(&s, &queue).await?;
        }

        let end_consume = Utc::now();
        let consume_took = end_consume.sub(start_consume);
        println!("Consuming took: {}", consume_took);

        let start_publish_and_consume = Utc::now();

        let mut pending = Vec::with_capacity(queue_count);

        for i in 0..queue_count {
            let queue = format!("test-queue-{}", i);
            let queue2 = queue.clone();
            let handle_publish = tokio::spawn(async move { publish_messages(i, queue2).await.unwrap() });
            let queue2 = queue.clone();
            let handle_consume = tokio::spawn(async move { consume_messages(i, queue2, Some(10)).await.unwrap() });
            pending.push((queue, handle_publish, handle_consume));
        }

        for (queue, work_publish, work_consume) in pending {
            work_publish.await?;
            work_consume.await?;
            check_queue_empty(&s, &queue).await?;
        }

        let end_publish_and_consume = Utc::now();
        let publish_and_consume_took = end_publish_and_consume.sub(start_publish_and_consume);
        println!("Publishing and consuming took: {}", publish_and_consume_took);

        Ok(())
    })
}

async fn check_queue_empty(s: &Service, queue: &str) -> Result<(), AnyError> {
    let info = s.describe_queue(queue, None).await?;
    if let Some(description) = info {
        if description.status.messages != 0 {
            Err(StringError::new("Queue not yet empty"))?
        } else {
            Ok(())
        }
    } else {
        Err(StringError::new("Failed to describe queue"))?
    }
}

const DEFAULT_MESSAGE: [&'static [u8]; 3] = [
    "{\"data\":\"a test\"}".as_bytes(),
    &[6, 3, 6, 8, 0, 0, 0, 1, 5, 22, 254, 0, 32, 100, 128, 0],
    &[1, 2, 5, 3, 7, 4, 98, 66, 127, 0, 255, 192],
];
const DEFAULT_TRACE_ID: [Option<Uuid>; 3] = [
    Some(Uuid::NAMESPACE_DNS), // some (not so) random uuid
    Some(Uuid::NAMESPACE_URL),
    None,
];
const DEFAULT_MESSAGE_CONTENT_TYPE: [&'static str; 3] = ["application/json", "text/html", "image/png"];
const DEFAULT_MESSAGE_CONTENT_ENCODING: [Option<&'static str>; 3] = [Some("identity"), Some("gzip"), None];

async fn publish_messages(index: usize, queue: String) -> Result<(), AnyError> {
    let s = get_service();
    let message = DEFAULT_MESSAGE[index % DEFAULT_MESSAGE.len()].to_owned();
    let mut message_bundle = Vec::with_capacity(if index > 5 { 10 } else { 1 });
    for _ in 0..message_bundle.capacity() {
        message_bundle.push(PublishableMessage {
            content_type:     DEFAULT_MESSAGE_CONTENT_TYPE[index % DEFAULT_MESSAGE_CONTENT_TYPE.len()],
            content_encoding: DEFAULT_MESSAGE_CONTENT_ENCODING[index % DEFAULT_MESSAGE_CONTENT_ENCODING.len()],
            trace_id:         DEFAULT_TRACE_ID[index % DEFAULT_TRACE_ID.len()],
            message:          message.clone(),
        });
    }
    for _ in 0..10000 / message_bundle.len() {
        let result = s.publish_messages(&queue, &message_bundle).await?;
        if !result {
            Err(StringError::new("Expected successful publish"))?;
        }
    }

    Ok(())
}

async fn consume_messages(index: usize, queue: String, timeout: Option<u16>) -> Result<(), AnyError> {
    let mut handles = Vec::with_capacity(NUM_THREADS);
    for _ in 0..handles.capacity() {
        let queue_name = queue.clone();
        let handle = tokio::spawn(async move {
            let s = get_service();
            loop {
                let messages = s.get_messages(&queue_name, 10, timeout).await?;
                if messages.is_empty() {
                    break;
                }
                for message in messages {
                    if &message.content_type != DEFAULT_MESSAGE_CONTENT_TYPE[index % DEFAULT_MESSAGE_CONTENT_TYPE.len()]
                    {
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
                    if message.trace_id != DEFAULT_TRACE_ID[index % DEFAULT_TRACE_ID.len()] {
                        Err(StringError::new("Message trace id does not match"))?;
                    }
                    if message.content.as_slice() != DEFAULT_MESSAGE[index % DEFAULT_MESSAGE.len()] {
                        Err(StringError::new("Message content does not match"))?;
                    }
                    let deleted = s.delete_message(None, &message.message_id).await?;

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
