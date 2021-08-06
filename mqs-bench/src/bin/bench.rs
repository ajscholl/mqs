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
use tokio::{runtime::Builder, time::sleep};
use uuid::Uuid;

use mqs_client::{ClientError, PublishableMessage, Service};
use mqs_common::QueueConfig;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

type AnyError = Box<dyn Error + Send + Sync>;

const NUM_THREADS: usize = 25;

#[derive(Debug)]
struct StringError {
    error: &'static str,
}

impl StringError {
    const fn new(error: &'static str) -> Self {
        Self { error }
    }
}

impl Display for StringError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl Error for StringError {}

fn get_service() -> Service {
    let host = env::var("MQS_SERVER").unwrap_or_else(|_| "localhost".to_string());
    Service::new(&format!("http://{}:7843", &host))
}

fn main() -> Result<(), AnyError> {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(NUM_THREADS)
        .build()
        .unwrap();

    rt.block_on(async {
        {
            if let Err(ClientError::TooLargeResponse) = get_service()
                .set_max_body_size(Some(1))
                .get_queues(None, None, None)
                .await
            {
                // this is the expected case, don't fail
            } else {
                return Err(StringError::new("request should have been aborted").into());
            }
        }
        let s = get_service();
        let queue_count: usize = 10;
        let queues = s.get_queues(None, None, None).await?;
        // clear data
        for queue in queues.queues {
            let result = s.delete_queue(&queue.name, None).await?;

            if result.is_none() {
                return Err(StringError::new("Failed to delete queue").into());
            }
        }

        create_queues(queue_count).await?;
        update_queues(queue_count).await?;

        let start_publish = Utc::now();
        publish_test_messages(queue_count).await?;
        let start_consume = Utc::now();
        let publish_took = start_consume.sub(start_publish);
        println!("Publishing took: {}", publish_took);

        consume_test_messages(queue_count).await?;
        let end_consume = Utc::now();
        let consume_took = end_consume.sub(start_consume);
        println!("Consuming took: {}", consume_took);

        let start_publish_and_consume = Utc::now();
        publish_and_consume_test_messages(queue_count).await?;
        let end_publish_and_consume = Utc::now();
        let publish_and_consume_took = end_publish_and_consume.sub(start_publish_and_consume);
        println!("Publishing and consuming took: {}", publish_and_consume_took);

        Ok(())
    })
}

async fn create_queues(queue_count: usize) -> Result<(), AnyError> {
    let s = get_service();

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
            return Err(StringError::new("Failed to create queue").into());
        }
    }

    Ok(())
}

async fn update_queues(queue_count: usize) -> Result<(), AnyError> {
    let s = get_service();

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
            return Err(StringError::new("Failed to update queue").into());
        }
    }

    Ok(())
}

async fn publish_test_messages(queue_count: usize) -> Result<(), AnyError> {
    let s = get_service();
    let mut pending = Vec::with_capacity(queue_count);

    for i in 0..queue_count {
        let queue = format!("test-queue-{}", i);
        let queue_copy = queue.clone();
        let handle = tokio::spawn(async move { publish_messages(i, queue_copy).await.unwrap() });
        pending.push((queue, handle));
    }

    for (queue, work) in pending {
        work.await?;
        let info = s.describe_queue(&queue, None).await?;
        if let Some(description) = info {
            if description.status.messages != 10000 {
                return Err(StringError::new("Wrong number of messages found").into());
            }
        } else {
            return Err(StringError::new("Failed to describe queue").into());
        }
    }

    Ok(())
}

async fn consume_test_messages(queue_count: usize) -> Result<(), AnyError> {
    let s = get_service();
    let mut pending = Vec::with_capacity(queue_count);

    for i in 0..queue_count {
        let queue = format!("test-queue-{}", i);
        let queue_copy = queue.clone();
        let publish_done = Arc::new(AtomicBool::new(true));
        let handle = tokio::spawn(async move {
            consume_messages(i, queue_copy, None, NUM_THREADS, publish_done.clone())
                .await
                .unwrap();
        });
        pending.push((queue, handle));
    }

    for (queue, work) in pending {
        work.await?;
        check_queue_empty(&s, &queue).await?;
    }

    Ok(())
}

async fn publish_and_consume_test_messages(queue_count: usize) -> Result<(), AnyError> {
    let s = get_service();
    let mut pending = Vec::with_capacity(queue_count);

    for i in 0..queue_count {
        let queue = format!("test-queue-{}", i);
        let queue_copy = queue.clone();
        let publish_done = Arc::new(AtomicBool::new(false));
        let publisher_done = publish_done.clone();
        let handle_publish = tokio::spawn(async move {
            publish_messages(i, queue_copy).await.unwrap();
            publisher_done.store(true, Ordering::Relaxed);
        });
        let queue_copy = queue.clone();
        let handle_consume = tokio::spawn(async move {
            consume_messages(i, queue_copy, Some(10), 1, publish_done.clone())
                .await
                .unwrap();
        });
        pending.push((queue, handle_publish, handle_consume));
    }

    for (queue, work_publish, work_consume) in pending {
        work_publish.await?;
        work_consume.await?;
        check_queue_empty(&s, &queue).await?;
    }

    Ok(())
}

async fn check_queue_empty(s: &Service, queue: &str) -> Result<(), AnyError> {
    let info = s.describe_queue(queue, None).await?;
    info.map_or_else(
        || Err(StringError::new("Failed to describe queue").into()),
        |description| {
            if description.status.messages == 0 {
                Ok(())
            } else {
                Err(StringError::new("Queue not yet empty").into())
            }
        },
    )
}

const DEFAULT_MESSAGE: [&[u8]; 3] = [
    b"{\"data\":\"a test\"}",
    &[6, 3, 6, 8, 0, 0, 0, 1, 5, 22, 254, 0, 32, 100, 128, 0],
    &[1, 2, 5, 3, 7, 4, 98, 66, 127, 0, 255, 192],
];
const DEFAULT_TRACE_ID: [Option<Uuid>; 3] = [
    Some(Uuid::NAMESPACE_DNS), // some (not so) random uuid
    Some(Uuid::NAMESPACE_URL),
    None,
];
const DEFAULT_MESSAGE_CONTENT_TYPE: [&str; 3] = ["application/json", "text/html", "image/png"];
const DEFAULT_MESSAGE_CONTENT_ENCODING: [Option<&str>; 3] = [Some("identity"), Some("gzip"), None];

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
    let mut published_messages = 0;
    for _ in 0..10000 / message_bundle.len() {
        let result = s.publish_messages(&queue, &message_bundle).await?;
        if !result {
            return Err(StringError::new("Expected successful publish").into());
        }
        published_messages += message_bundle.len();
    }

    println!("Published {} messages to queue {}", published_messages, &queue);

    Ok(())
}

async fn consume_messages(
    index: usize,
    queue: String,
    timeout: Option<u16>,
    workers: usize,
    publish_done: Arc<AtomicBool>,
) -> Result<(), AnyError> {
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..handles.capacity() {
        let queue_name = queue.clone();
        let publisher_done = publish_done.clone();
        let handle = tokio::spawn(async move { consume_worker(index, timeout, publisher_done, queue_name).await });
        handles.push(handle);
    }

    let mut consumed_messages = 0;
    let mut min_consumed = i32::MAX;
    let mut max_consumed = 0;
    for handle in handles {
        let consumed = handle.await??;
        consumed_messages += consumed;
        min_consumed = min_consumed.min(consumed);
        max_consumed = max_consumed.max(consumed);
    }

    println!(
        "Consumed {} messages (between {} and {} / worker) with {} workers in total from queue {}",
        consumed_messages, min_consumed, max_consumed, workers, &queue
    );

    Ok(())
}

async fn consume_worker(
    index: usize,
    timeout: Option<u16>,
    publish_done: Arc<AtomicBool>,
    queue_name: String,
) -> Result<i32, AnyError> {
    let s = get_service();
    let mut consumed_messages = 0;
    loop {
        let messages = s.get_messages(&queue_name, 10, timeout).await?;
        if messages.is_empty() {
            if publish_done.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
        for message in messages {
            consumed_messages += 1;
            if message.content_type != DEFAULT_MESSAGE_CONTENT_TYPE[index % DEFAULT_MESSAGE_CONTENT_TYPE.len()] {
                return Err(StringError::new("Message content type does not match").into());
            }
            match DEFAULT_MESSAGE_CONTENT_ENCODING[index % DEFAULT_MESSAGE_CONTENT_ENCODING.len()] {
                None => {
                    if message.content_encoding.is_some() {
                        return Err(StringError::new("Unexpected message content encoding").into());
                    }
                },
                Some(encoding) => {
                    if message.content_encoding.is_none() {
                        return Err(StringError::new("Message content encoding missing").into());
                    }
                    if message.content_encoding.unwrap().as_str() != encoding {
                        return Err(StringError::new("Message content encoding does not match").into());
                    }
                },
            }
            if message.trace_id != DEFAULT_TRACE_ID[index % DEFAULT_TRACE_ID.len()] {
                return Err(StringError::new("Message trace id does not match").into());
            }
            if message.message_receives != 1 {
                return Err(StringError::new("Message was received wrong number of times").into());
            }
            if message.content.as_slice() != DEFAULT_MESSAGE[index % DEFAULT_MESSAGE.len()] {
                return Err(StringError::new("Message content does not match").into());
            }
            let deleted = s.delete_message(None, &message.message_id).await?;

            if !deleted {
                return Err(StringError::new("Failed to delete message").into());
            }
        }
    }

    Ok::<i32, AnyError>(consumed_messages)
}
