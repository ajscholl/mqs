use crate::connection::DBConn;

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
    pub fn new(conn: DBConn) -> Self {
        Self { conn }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        connection::Source,
        models::{
            health::HealthCheckRepository,
            message::{Message, MessageInput, MessageRepository},
            queue::{pg_interval, Queue, QueueDescription, QueueInput, QueueRepository, QueueSource},
        },
    };
    use diesel::QueryResult;
    use mqs_common::UtcTime;
    use serde::de::StdError;
    use sha2::{Digest, Sha256};
    use std::{
        cell::Cell,
        collections::HashMap,
        fmt::{Display, Formatter},
        ops::Deref,
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

    pub(crate) struct TestRepo {
        health:   bool,
        next_id:  Cell<i32>,
        queues:   Cell<HashMap<String, Queue>>,
        messages: Cell<HashMap<Uuid, Message>>,
    }

    impl TestRepo {
        pub fn new() -> Self {
            TestRepo {
                health:   true,
                next_id:  Cell::new(1),
                queues:   Cell::new(HashMap::new()),
                messages: Cell::new(HashMap::new()),
            }
        }

        pub fn set_health(&mut self, health: bool) {
            self.health = health;
        }

        fn next_id(&self) -> i32 {
            let id = self.next_id.get();
            self.next_id.set(id + 1);

            id
        }

        fn with_queues<R, F: FnOnce(&HashMap<String, Queue>) -> R>(&self, f: F) -> R {
            self.with_queues_mut(|queues| f(queues))
        }

        fn with_queues_mut<R, F: FnOnce(&mut HashMap<String, Queue>) -> R>(&self, f: F) -> R {
            // not the fastest way, but this is only test code and like this we can avoid
            // using unsafe code (which I prefer in tests)
            let mut queues = self.queues.replace(HashMap::new());
            let r = f(&mut queues);
            self.queues.replace(queues);
            r
        }

        fn with_messages<R, F: FnOnce(&HashMap<Uuid, Message>) -> R>(&self, f: F) -> R {
            self.with_messages_mut(|messages| f(messages))
        }

        fn with_messages_mut<R, F: FnOnce(&mut HashMap<Uuid, Message>) -> R>(&self, f: F) -> R {
            let mut messages = self.messages.replace(HashMap::new());
            let r = f(&mut messages);
            self.messages.replace(messages);
            r
        }
    }

    impl HealthCheckRepository for TestRepo {
        fn check_health(&self) -> bool {
            self.health
        }
    }

    impl MessageRepository for TestRepo {
        fn insert_message(&self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
            let mut found = false;
            let hash = if queue.content_based_deduplication {
                let mut digest = Sha256::default();
                digest.update(input.payload);
                let result = base64::encode(digest.finalize().as_slice());
                self.with_messages(|messages| {
                    for message in messages.values() {
                        if let Some(msg_hash) = &message.hash {
                            if msg_hash == &result {
                                found = true;
                                return None;
                            }
                        }
                    }

                    Some(result)
                })
            } else {
                None
            };
            if found {
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
            self.with_messages_mut(|messages| messages.insert(message.id.clone(), message));

            Ok(true)
        }

        fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
            let mut result: Vec<Message> = Vec::with_capacity(count as usize);
            let now = UtcTime::now();

            self.with_messages_mut(|messages| {
                for message in messages.values_mut() {
                    if message.visible_since > now || &message.queue != &queue.name {
                        continue;
                    }

                    message.receives += 1;
                    message.visible_since = now.add_pg_interval(&queue.visibility_timeout);
                    result.push(message.clone());
                }
            });

            Ok(result)
        }

        fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
            let mut modified = 0;

            self.with_messages_mut(|messages| {
                for id in ids {
                    match messages.get_mut(&id) {
                        None => {},
                        Some(msg) => {
                            msg.queue = new_queue.to_string();
                            modified += 1;
                        },
                    }
                }
            });

            Ok(modified)
        }

        fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool> {
            Ok(self.with_messages_mut(|messages| messages.remove(&id).is_some()))
        }

        fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize> {
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
        fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(self.with_queues(|queues| queues.get(&name.to_string()).map(|queue| queue.clone())))
        }
    }

    impl QueueRepository for TestRepo {
        fn insert_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
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
            self.with_queues_mut(|queues| {
                queues.insert(queue.name.to_string(), queue.clone());
            });

            Ok(Some(queue))
        }

        fn count_queues(&self) -> QueryResult<i64> {
            Ok(self.with_queues(|queues| queues.len() as i64))
        }

        fn describe_queue(&self, name: &str) -> QueryResult<Option<QueueDescription>> {
            let queue = self.find_by_name(name)?;
            if let Some(queue) = queue {
                let mut messages_count = 0;
                let mut visible_messages = 0;
                let mut oldest_message_age = 0;
                let now = UtcTime::now();

                self.with_messages(|messages| {
                    for message in messages.values() {
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
                })
            } else {
                Ok(None)
            }
        }

        fn list_queues(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
            let mut skip = offset.unwrap_or(0);
            let max = limit.unwrap_or(self.with_queues(|queues| queues.len() as i64)) as usize;
            let mut result = Vec::with_capacity(max);

            self.with_queues(|queues| {
                for queue in queues.values() {
                    if skip > 0 {
                        skip -= 0;
                        continue;
                    }

                    result.push(queue.clone());
                    if result.len() == max {
                        break;
                    }
                }
            });

            Ok(result)
        }

        fn update_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
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
                self.with_queues_mut(|queues| {
                    queues.insert(queue.name.to_string(), queue.clone());
                });

                Ok(Some(queue))
            } else {
                Ok(None)
            }
        }

        fn delete_queue_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(self.with_queues_mut(|queues| queues.remove(name)))
        }
    }

    pub(crate) struct CloneSource<R> {
        repo: R,
    }

    impl<R: Clone> CloneSource<R> {
        pub(crate) fn new(repo: &R) -> Self {
            CloneSource { repo: repo.clone() }
        }
    }

    impl<R: Clone + Send> Source<R> for CloneSource<R> {
        fn get(&self) -> Option<R> {
            Some(self.repo.clone())
        }
    }

    impl<R: HealthCheckRepository + Sync> HealthCheckRepository for Arc<R> {
        fn check_health(&self) -> bool {
            self.deref().check_health()
        }
    }

    impl<R: HealthCheckRepository> HealthCheckRepository for Mutex<R> {
        fn check_health(&self) -> bool {
            self.lock().unwrap().check_health()
        }
    }

    impl<R: QueueSource + Sync> QueueSource for Arc<R> {
        fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            self.deref().find_by_name(name)
        }
    }

    impl<R: QueueSource> QueueSource for Mutex<R> {
        fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            self.lock().unwrap().find_by_name(name)
        }
    }

    impl<R: QueueRepository + Sync> QueueRepository for Arc<R> {
        fn insert_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            self.deref().insert_queue(queue)
        }

        fn count_queues(&self) -> QueryResult<i64> {
            self.deref().count_queues()
        }

        fn describe_queue(&self, name: &str) -> QueryResult<Option<QueueDescription>> {
            self.deref().describe_queue(name)
        }

        fn list_queues(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
            self.deref().list_queues(offset, limit)
        }

        fn update_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            self.deref().update_queue(queue)
        }

        fn delete_queue_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            self.deref().delete_queue_by_name(name)
        }
    }

    impl<R: QueueRepository> QueueRepository for Mutex<R> {
        fn insert_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            self.lock().unwrap().insert_queue(queue)
        }

        fn count_queues(&self) -> QueryResult<i64> {
            self.lock().unwrap().count_queues()
        }

        fn describe_queue(&self, name: &str) -> QueryResult<Option<QueueDescription>> {
            self.lock().unwrap().describe_queue(name)
        }

        fn list_queues(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
            self.lock().unwrap().list_queues(offset, limit)
        }

        fn update_queue(&self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
            self.lock().unwrap().update_queue(queue)
        }

        fn delete_queue_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            self.lock().unwrap().delete_queue_by_name(name)
        }
    }

    impl<R: MessageRepository + Sync> MessageRepository for Arc<R> {
        fn insert_message(&self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
            self.deref().insert_message(queue, input)
        }

        fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
            self.deref().get_message_from_queue(queue, count)
        }

        fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
            self.deref().move_message_to_queue(ids, new_queue)
        }

        fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool> {
            self.deref().delete_message_by_id(id)
        }

        fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize> {
            self.deref().delete_messages_by_ids(ids)
        }
    }

    impl<R: MessageRepository> MessageRepository for Mutex<R> {
        fn insert_message(&self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
            self.lock().unwrap().insert_message(queue, input)
        }

        fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
            self.lock().unwrap().get_message_from_queue(queue, count)
        }

        fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
            self.lock().unwrap().move_message_to_queue(ids, new_queue)
        }

        fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool> {
            self.lock().unwrap().delete_message_by_id(id)
        }

        fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize> {
            self.lock().unwrap().delete_messages_by_ids(ids)
        }
    }
}
