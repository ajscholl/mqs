use mqs_common::connection::DBConn;

pub mod health;
pub mod message;
pub mod queue;

/// A `PgRepository` implements the different repository traits to provide a database access layer
/// for the different request handlers.
pub struct PgRepository {
    conn: DBConn,
}

impl PgRepository {
    /// Create a new repository with the given database connection.
    #[must_use]
    pub fn new(conn: DBConn) -> Self {
        Self { conn }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::models::{
        health::HealthCheckRepository,
        message::{Message, MessageInput, MessageRepository},
        queue::{pg_interval, Queue, QueueDescription, QueueInput, QueueRepository, QueueSource},
    };
    use diesel::QueryResult;
    use mqs_common::{connection::Source, UtcTime};
    use serde::de::StdError;
    use sha2::{Digest, Sha256};
    use std::{
        cell::Cell,
        collections::HashMap,
        fmt::{Display, Formatter},
        mem::swap,
        sync::{Arc, Mutex},
    };
    use uuid::Uuid;

    #[derive(Debug)]
    struct TestError {
        message: &'static str,
    }

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.message)
        }
    }

    impl StdError for TestError {}

    pub(crate) struct TestRepoSource {
        repo: Arc<Mutex<Cell<Option<TestRepoData>>>>,
    }

    impl TestRepoSource {
        pub(crate) fn new() -> Self {
            TestRepoSource {
                repo: Arc::new(Mutex::new(Cell::new(Some(TestRepoData::new())))),
            }
        }
    }

    impl Source<TestRepo> for TestRepoSource {
        fn get(&self) -> Option<TestRepo> {
            let cell = self.repo.lock().expect("Failed to lock test repo source cell");
            let data = cell.replace(None).expect("Test repo source cell was empty");

            Some(TestRepo {
                source_repo: Arc::clone(&self.repo),
                data,
            })
        }
    }

    struct TestRepoData {
        health:   bool,
        next_id:  i32,
        queues:   HashMap<String, Queue>,
        messages: HashMap<Uuid, Message>,
    }

    pub(crate) struct TestRepo {
        source_repo: Arc<Mutex<Cell<Option<TestRepoData>>>>,
        data:        TestRepoData,
    }

    impl TestRepoData {
        pub fn new() -> Self {
            TestRepoData {
                health:   true,
                next_id:  1,
                queues:   HashMap::new(),
                messages: HashMap::new(),
            }
        }
    }

    impl TestRepo {
        pub fn set_health(&mut self, health: bool) {
            self.data.health = health;
        }

        fn next_id(&mut self) -> i32 {
            let id = self.data.next_id;
            self.data.next_id = id + 1;

            id
        }

        fn get_hash_and_duplicate(&self, queue: &Queue, input: &MessageInput<'_>) -> (Option<String>, bool) {
            if queue.content_based_deduplication {
                let mut digest = Sha256::default();
                digest.update(input.payload);
                let result = base64::encode(digest.finalize().as_slice());
                for message in self.data.messages.values() {
                    if let Some(msg_hash) = &message.hash {
                        if msg_hash == &result {
                            return (None, true);
                        }
                    }
                }

                (Some(result), false)
            } else {
                (None, false)
            }
        }
    }

    // when we are done with a test repo, we "commit" the data to the "database" by moving it back to
    // the source and thus enable us to access the "database" another time with another task.
    impl Drop for TestRepo {
        fn drop(&mut self) {
            let mut data = TestRepoData::new();
            swap(&mut data, &mut self.data);

            let cell = self.source_repo.lock().expect("Failed to lock test repo source cell");
            let old_data = cell.replace(Some(data));
            debug_assert!(
                old_data.is_none(),
                "There was already data in the test repo source cell"
            )
        }
    }

    impl HealthCheckRepository for TestRepo {
        fn check_health(&mut self) -> bool {
            self.data.health
        }
    }

    impl MessageRepository for TestRepo {
        fn insert_message(&mut self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
            let (hash, has_duplicate) = self.get_hash_and_duplicate(queue, input);
            if has_duplicate {
                return Ok(false);
            }
            let now = UtcTime::now();
            let message = Message {
                id: Uuid::new_v4(),
                payload: input.payload.to_vec(),
                content_type: input.content_type.to_string(),
                content_encoding: input.content_encoding.map(|s| s.to_string()),
                hash,
                queue: queue.name.to_string(),
                receives: 0,
                visible_since: now.add_pg_interval(&queue.message_delay),
                created_at: now,
                trace_id: None,
            };
            self.data.messages.insert(message.id.clone(), message);

            Ok(true)
        }

        fn get_message_from_queue(&mut self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
            let mut result: Vec<Message> = Vec::with_capacity(count as usize);
            let now = UtcTime::now();

            for message in self.data.messages.values_mut() {
                if message.visible_since > now || &message.queue != &queue.name {
                    continue;
                }

                message.receives += 1;
                message.visible_since = now.add_pg_interval(&queue.visibility_timeout);
                result.push(message.clone());
            }

            Ok(result)
        }

        fn move_message_to_queue(&mut self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
            let mut modified = 0;

            for id in ids {
                match self.data.messages.get_mut(&id) {
                    None => {},
                    Some(msg) => {
                        msg.queue = new_queue.to_string();
                        modified += 1;
                    },
                }
            }

            Ok(modified)
        }

        fn delete_message_by_id(&mut self, id: Uuid) -> QueryResult<bool> {
            Ok(self.data.messages.remove(&id).is_some())
        }

        fn delete_messages_by_ids(&mut self, ids: Vec<Uuid>) -> QueryResult<usize> {
            let mut deleted = 0;

            for id in ids {
                if self.delete_message_by_id(id)? {
                    deleted += 1;
                }
            }

            Ok(deleted)
        }
    }

    impl QueueSource for TestRepo {
        fn find_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(self.data.queues.get(&name.to_string()).map(|queue| queue.clone()))
        }
    }

    impl QueueRepository for TestRepo {
        fn insert_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            if self.find_by_name(queue.name)?.is_some() {
                return Ok(None);
            }
            let now = UtcTime::now();
            let queue = Queue {
                id:                          self.next_id(),
                name:                        queue.name.to_string(),
                max_receives:                queue.max_receives,
                dead_letter_queue:           queue.dead_letter_queue.map(|s| s.to_string()),
                retention_timeout:           pg_interval(queue.retention_timeout),
                visibility_timeout:          pg_interval(queue.visibility_timeout),
                message_delay:               pg_interval(queue.message_delay),
                content_based_deduplication: queue.content_based_deduplication,
                created_at:                  now,
                updated_at:                  now,
            };
            self.data.queues.insert(queue.name.to_string(), queue.clone());

            Ok(Some(queue))
        }

        fn count_queues(&mut self) -> QueryResult<i64> {
            Ok(self.data.queues.len() as i64)
        }

        fn describe_queue(&mut self, name: &str) -> QueryResult<Option<QueueDescription>> {
            let queue = self.find_by_name(name)?;
            if let Some(queue) = queue {
                let mut messages_count = 0;
                let mut visible_messages = 0;
                let mut oldest_message_age = 0;
                let now = UtcTime::now();

                for message in self.data.messages.values() {
                    if &message.queue != &queue.name {
                        continue;
                    }

                    messages_count += 1;
                    visible_messages += if message.visible_since <= now { 1 } else { 0 };
                    if let Ok(message_age) = now.since(&message.created_at) {
                        oldest_message_age = oldest_message_age.max(message_age.as_secs());
                    }
                }

                Ok(Some(QueueDescription {
                    queue,
                    messages: messages_count,
                    visible_messages,
                    oldest_message_age,
                }))
            } else {
                Ok(None)
            }
        }

        fn list_queues(&mut self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
            let mut skip = offset.unwrap_or(0);
            let max = limit.unwrap_or(self.data.queues.len() as i64) as usize;
            let mut result = Vec::with_capacity(max);

            for queue in self.data.queues.values() {
                if skip > 0 {
                    skip -= 0;
                    continue;
                }

                result.push(queue.clone());
                if result.len() == max {
                    break;
                }
            }

            Ok(result)
        }

        fn update_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            let old = self.find_by_name(queue.name)?;
            if let Some(old) = old {
                let queue = Queue {
                    id:                          old.id,
                    name:                        queue.name.to_string(),
                    max_receives:                queue.max_receives,
                    dead_letter_queue:           queue.dead_letter_queue.map(|s| s.to_string()),
                    retention_timeout:           pg_interval(queue.retention_timeout),
                    visibility_timeout:          pg_interval(queue.visibility_timeout),
                    message_delay:               pg_interval(queue.message_delay),
                    content_based_deduplication: queue.content_based_deduplication,
                    created_at:                  old.created_at,
                    updated_at:                  UtcTime::now(),
                };
                self.data.queues.insert(queue.name.to_string(), queue.clone());

                Ok(Some(queue))
            } else {
                Ok(None)
            }
        }

        fn delete_queue_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(self.data.queues.remove(name))
        }
    }
}
