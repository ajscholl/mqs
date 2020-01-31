extern crate chrono;
extern crate hyper;

use mqs::client::Service;
use mqs::routes::queues::QueueConfig;

use std::error::Error;
use std::fmt::{Display, Formatter};
use chrono::Utc;
use std::ops::Sub;

type AnyError = Box<dyn std::error::Error + Send + Sync>;

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

unsafe impl Send for StringError {}
unsafe impl Sync for StringError {}
impl Error for StringError {}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let s = Service::new("http://localhost:7843");
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
        let handle = tokio::spawn(async {
            publish_messages(queue2).await.unwrap()
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
        let handle = tokio::spawn(async {
            consume_messages(queue2).await.unwrap()
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
}

async fn publish_messages(queue: String) -> Result<(), AnyError> {
    let s = Service::new("http://localhost:7843");
    let message = "{\"data\":\"a test\"}".as_bytes().to_owned();
    for _ in 0..10000 {
        let result = s.publish_message(&queue, "application/json", message.clone()).await?;
        if !result {
            Err(StringError::new("Expected successful publish"))?;
        }
    }

    Ok(())
}

async fn consume_messages(queue: String) -> Result<(), AnyError> {
    let mut handles = Vec::with_capacity(10);
    for _ in 0..handles.capacity() {
        let queue_name = queue.clone();
        let handle = tokio::spawn(async {
            let s = Service::new("http://localhost:7843");
            let queue2 = queue_name;
            while let Some(message) = s.get_message(&queue2).await? {
                let deleted = s.delete_message(&message.message_id).await?;

                if !deleted {
                    Err(StringError::new("Failed to delete message"))?;
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
