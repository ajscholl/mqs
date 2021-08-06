use cached::once_cell::sync::Lazy;
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{oneshot, oneshot::Sender, Mutex},
    time::timeout,
};
use uuid::Uuid;

use crate::models::queue::Queue;

type MessageWaitQueueMap = HashMap<String, HashMap<Uuid, Sender<()>>>;

pub struct MessageWaitQueue {
    wait_queue: Mutex<MessageWaitQueueMap>,
}

impl MessageWaitQueue {
    pub fn new() -> Self {
        Self {
            wait_queue: Mutex::new(HashMap::new()),
        }
    }

    pub async fn wait(&self, queue: &Queue, max_wait_time: u64) -> bool {
        let (tx, rx) = oneshot::channel();
        let queue_name = queue.name.to_string();
        let id = Uuid::new_v4();

        {
            debug!(
                "Waiting {} seconds for a new message on queue {} (id {})",
                max_wait_time,
                &queue_name,
                id.to_string()
            );
            let mut guard = self.wait_queue.lock().await;
            let map: &mut MessageWaitQueueMap = &mut *guard;
            match map.get_mut(&queue_name) {
                None => {
                    let mut waiting = HashMap::new();
                    waiting.insert(id, tx);
                    map.insert(queue_name.clone(), waiting);
                },
                Some(waiting) => {
                    waiting.insert(id, tx);
                },
            }
        }

        let found = timeout(Duration::from_secs(max_wait_time), rx).await.is_ok();
        debug!(
            "Done waiting {} seconds for a new message on queue {} (id {}): {}",
            max_wait_time,
            &queue_name,
            id.to_string(),
            found
        );

        {
            let mut guard = self.wait_queue.lock().await;
            let map: &mut MessageWaitQueueMap = &mut *guard;
            match map.get_mut(&queue_name) {
                None => {
                    error!(
                        "Someone else removed our queue {} from the waiting map (id {})",
                        &queue_name,
                        id.to_string()
                    );
                },
                Some(waiting) => {
                    waiting.remove(&id);
                    if waiting.is_empty() {
                        map.remove(&queue_name);
                        debug!(
                            "Removing waiting entries for queue {}: It is empty (id {})",
                            &queue_name,
                            id.to_string()
                        );
                    }
                },
            }
        }

        found
    }

    pub async fn signal(&self, queue: &Queue) {
        let mut guard = self.wait_queue.lock().await;
        let map: &mut MessageWaitQueueMap = &mut *guard;
        match map.get_mut(&queue.name.to_string()) {
            None => {
                debug!("Not signaling on queue {}: No waiting entries", &queue.name);
            },
            Some(waiting) => {
                let key = waiting.keys().next().map_or_else(|| None, |k| Some(*k));
                if let Some(key) = key {
                    if let Some(value) = waiting.remove(&key) {
                        match value.send(()) {
                            Err(()) => {
                                debug!(
                                    "Failed to signal id {} on queue {}: It no longer listens for our signal",
                                    key.to_string(),
                                    &queue.name
                                );
                            },
                            Ok(()) => {
                                debug!("Signaled id {} on queue {}", key.to_string(), &queue.name);
                            },
                        };
                    }
                }
            },
        }
    }
}

pub static MESSAGE_WAIT_QUEUE: Lazy<MessageWaitQueue> = Lazy::new(MessageWaitQueue::new);

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::models::queue::pg_interval;
    use chrono::Utc;
    use mqs_common::test::make_runtime;
    use std::time::Duration;
    use tokio::time::sleep;

    fn get_queue() -> Queue {
        Queue {
            id:                          1,
            name:                        "test-queue".to_string(),
            max_receives:                None,
            dead_letter_queue:           None,
            retention_timeout:           pg_interval(30),
            visibility_timeout:          pg_interval(30),
            message_delay:               pg_interval(30),
            content_based_deduplication: false,
            created_at:                  Utc::now().naive_utc(),
            updated_at:                  Utc::now().naive_utc(),
        }
    }

    #[test]
    fn wait_no_signal() {
        let rt = make_runtime();
        let wait_queue = MessageWaitQueue::new();
        let signaled = rt.block_on(async { wait_queue.wait(&get_queue(), 1).await });
        assert!(!signaled);
    }

    #[test]
    fn wait_no_signal_after_signal() {
        let rt = make_runtime();
        let wait_queue = MessageWaitQueue::new();
        let signaled = rt.block_on(async {
            wait_queue.signal(&get_queue()).await;
            wait_queue.wait(&get_queue(), 1).await
        });
        assert!(!signaled);
    }

    #[test]
    fn wait_signal() {
        let rt = make_runtime();
        static WAIT_QUEUE: Lazy<MessageWaitQueue> = Lazy::new(MessageWaitQueue::new);
        rt.spawn(async {
            sleep(Duration::from_secs(2)).await;
            WAIT_QUEUE.signal(&get_queue()).await;
        });
        let signaled = rt.block_on(async { WAIT_QUEUE.wait(&get_queue(), 5).await });
        assert!(signaled);
    }
}
