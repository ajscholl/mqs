use cached::{once_cell::sync::Lazy, stores::TimedCache, Cached};
use diesel::{
    pg::data_types::PgInterval,
    prelude::*,
    result::{DatabaseErrorKind, Error},
};
use mqs_common::{QueueConfig, QueueConfigOutput, QueueRedrivePolicy, UtcTime};
use std::{
    convert::TryFrom,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use crate::{
    models::PgRepository,
    schema::{messages, queues},
};

#[derive(Debug)]
pub struct QueueInput<'a> {
    pub name:                        &'a str,
    pub max_receives:                Option<i32>,
    pub dead_letter_queue:           Option<&'a str>,
    pub retention_timeout:           i64,
    pub visibility_timeout:          i64,
    pub message_delay:               i64,
    pub content_based_deduplication: bool,
}

impl<'a> QueueInput<'a> {
    pub(crate) fn new(config: &'a QueueConfig, queue_name: &'a str) -> Self {
        QueueInput {
            name:                        queue_name,
            max_receives:                config.redrive_policy.as_ref().map(|p| p.max_receives),
            dead_letter_queue:           config.redrive_policy.as_ref().map(|p| p.dead_letter_queue.as_str()),
            retention_timeout:           config.retention_timeout,
            visibility_timeout:          config.visibility_timeout,
            message_delay:               config.message_delay,
            content_based_deduplication: config.message_deduplication,
        }
    }
}

#[derive(Insertable)]
#[diesel(table_name = queues)]
pub struct NewQueue<'a> {
    pub name:                        &'a str,
    pub max_receives:                Option<i32>,
    pub dead_letter_queue:           Option<&'a str>,
    pub retention_timeout:           PgInterval,
    pub visibility_timeout:          PgInterval,
    pub message_delay:               PgInterval,
    pub content_based_deduplication: bool,
    pub created_at:                  UtcTime,
    pub updated_at:                  UtcTime,
}

#[derive(Queryable, Identifiable, Clone, Debug, PartialEq, Eq)]
pub struct Queue {
    pub id:                          i32,
    pub name:                        String,
    pub max_receives:                Option<i32>,
    pub dead_letter_queue:           Option<String>,
    pub retention_timeout:           PgInterval,
    pub visibility_timeout:          PgInterval,
    pub message_delay:               PgInterval,
    pub content_based_deduplication: bool,
    pub created_at:                  UtcTime,
    pub updated_at:                  UtcTime,
}

impl Queue {
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn into_config_output(self) -> QueueConfigOutput {
        QueueConfigOutput {
            name:                  self.name,
            redrive_policy:        match (self.dead_letter_queue, self.max_receives) {
                (Some(dead_letter_queue), Some(max_receives)) => Some(QueueRedrivePolicy {
                    max_receives,
                    dead_letter_queue,
                }),
                _ => None,
            },
            retention_timeout:     pg_interval_seconds(&self.retention_timeout),
            visibility_timeout:    pg_interval_seconds(&self.visibility_timeout),
            message_delay:         pg_interval_seconds(&self.message_delay),
            message_deduplication: self.content_based_deduplication,
        }
    }
}

const fn pg_interval_seconds(interval: &PgInterval) -> i64 {
    interval.microseconds / 1_000_000 + interval.days as i64 * (24 * 3600) + interval.months as i64 * (30 * 24 * 3600)
}

pub fn pg_interval(mut seconds: i64) -> PgInterval {
    if seconds < 0 {
        let int = pg_interval(-seconds);

        return PgInterval {
            microseconds: -int.microseconds,
            days:         -int.days,
            months:       -int.months,
        };
    }

    let mut days = seconds / (24 * 3600);
    seconds -= days * (24 * 3600);
    let months = days / 30;
    days -= months * 30;

    PgInterval {
        microseconds: seconds * 1_000_000,
        days:         i32::try_from(days).unwrap_or(i32::MAX),
        months:       i32::try_from(months).unwrap_or(i32::MAX),
    }
}

pub struct QueueDescription {
    pub queue:              Queue,
    pub messages:           i64,
    pub visible_messages:   i64,
    pub oldest_message_age: u64,
}

static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
static QUEUE_CACHE: Lazy<Mutex<TimedCache<String, Queue>>> = Lazy::new(|| Mutex::new(TimedCache::with_lifespan(10)));

pub trait QueueSource: Send {
    fn find_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>>;
    fn find_by_name_cached(&mut self, name: &str) -> QueryResult<Option<Queue>> {
        let key = name.to_string();
        if let Ok(mut cache) = QUEUE_CACHE.lock() {
            let res = Cached::cache_get(&mut *cache, &key);
            if let Some(cached_val) = res {
                let cache_hits = CACHE_HITS.fetch_add(1, Ordering::Relaxed) + 1;
                info!(
                    "Found queue of name {}, total {} cache hits",
                    &cached_val.name, cache_hits
                );
                return Ok(Some(cached_val.clone()));
            }
        } else {
            error!("Failed to get queue cache lock");
            return self.find_by_name(name);
        }
        let cache_misses = CACHE_MISSES.fetch_add(1, Ordering::Relaxed) + 1;
        info!("Getting queue of name {}, total {} cache misses", name, cache_misses);
        match self.find_by_name(name) {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some(queue)) => {
                QUEUE_CACHE.lock().map_or_else(
                    |err| {
                        error!("Failed to get queue cache lock: {}", err);
                    },
                    |mut cache| {
                        Cached::cache_set(&mut *cache, key, queue.clone());
                    },
                );
                Ok(Some(queue))
            },
        }
    }
}

pub trait QueueRepository: QueueSource {
    fn insert_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>>;
    fn count_queues(&mut self) -> QueryResult<i64>;
    fn describe_queue(&mut self, name: &str) -> QueryResult<Option<QueueDescription>>;
    fn list_queues(&mut self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>>;
    fn update_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>>;
    fn delete_queue_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>>;
}

impl QueueSource for PgRepository {
    fn find_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>> {
        queues::table
            .filter(queues::name.eq(name))
            .first::<Queue>(&mut self.conn)
            .optional()
    }
}

impl QueueRepository for PgRepository {
    fn insert_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
        let now = UtcTime::now();
        let result = diesel::dsl::insert_into(queues::table)
            .values(NewQueue {
                name:                        queue.name,
                max_receives:                queue.max_receives,
                dead_letter_queue:           queue.dead_letter_queue,
                retention_timeout:           pg_interval(queue.retention_timeout),
                visibility_timeout:          pg_interval(queue.visibility_timeout),
                message_delay:               pg_interval(queue.message_delay),
                content_based_deduplication: queue.content_based_deduplication,
                created_at:                  now,
                updated_at:                  now,
            })
            .returning(queues::all_columns)
            .get_result(&mut self.conn);
        match result {
            Ok(queue) => Ok(Some(queue)),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn count_queues(&mut self) -> QueryResult<i64> {
        queues::table.count().get_result(&mut self.conn)
    }

    fn describe_queue(&mut self, name: &str) -> QueryResult<Option<QueueDescription>> {
        match self.find_by_name(name)? {
            None => Ok(None),
            Some(queue) => {
                let messages = messages::table
                    .filter(messages::queue.eq(&queue.name))
                    .count()
                    .get_result(&mut self.conn)?;
                let now = UtcTime::now();
                let visible_messages = messages::table
                    .filter(messages::queue.eq(&queue.name).and(messages::visible_since.le(now)))
                    .count()
                    .get_result(&mut self.conn)?;
                let oldest_message: Option<UtcTime> = messages::table
                    .select(messages::created_at)
                    .filter(messages::queue.eq(&queue.name))
                    .limit(1)
                    .order(messages::created_at.asc())
                    .for_key_share()
                    .skip_locked()
                    .get_result(&mut self.conn)
                    .optional()?;

                Ok(Some(QueueDescription {
                    queue,
                    messages,
                    visible_messages,
                    oldest_message_age: oldest_message
                        .map_or(0, |created_at| now.since(&created_at).map_or(0, |d| d.as_secs())),
                }))
            },
        }
    }

    fn list_queues(&mut self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
        let query = queues::table.order(queues::id.asc());

        match offset {
            None => match limit {
                None => query.get_results(&mut self.conn),
                Some(limit) => query.limit(limit).get_results(&mut self.conn),
            },
            Some(offset) => match limit {
                None => query.offset(offset).get_results(&mut self.conn),
                Some(limit) => query.offset(offset).limit(limit).get_results(&mut self.conn),
            },
        }
    }

    fn update_queue(&mut self, queue: &QueueInput<'_>) -> QueryResult<Option<Queue>> {
        diesel::dsl::update(queues::table.filter(queues::name.eq(queue.name)))
            .set((
                queues::max_receives.eq(queue.max_receives),
                queues::dead_letter_queue.eq(queue.dead_letter_queue),
                queues::retention_timeout.eq(pg_interval(queue.retention_timeout)),
                queues::visibility_timeout.eq(pg_interval(queue.visibility_timeout)),
                queues::message_delay.eq(pg_interval(queue.message_delay)),
                queues::content_based_deduplication.eq(queue.content_based_deduplication),
                queues::updated_at.eq(UtcTime::now()),
            ))
            .returning(queues::all_columns)
            .get_result(&mut self.conn)
            .optional()
    }

    fn delete_queue_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>> {
        diesel::dsl::delete(queues::table.filter(queues::name.eq(name)))
            .returning(queues::all_columns)
            .get_result(&mut self.conn)
            .optional()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn cache_test() {
        let initial_hits = CACHE_HITS.load(Ordering::Relaxed);
        let initial_misses = CACHE_MISSES.load(Ordering::Relaxed);
        let mut repo = QueueSourceImpl {};
        let queue = repo.find_by_name_cached("my queue").unwrap().unwrap();
        // We would normally expect this to be equal, but if another test runs while our tests runs,
        // we can get a larger result than expected.
        assert!(CACHE_HITS.load(Ordering::Relaxed) >= initial_hits);
        assert!(CACHE_MISSES.load(Ordering::Relaxed) >= initial_misses + 1);
        let cached = repo.find_by_name_cached("my queue").unwrap().unwrap();
        assert_eq!(queue, cached);
        assert!(CACHE_HITS.load(Ordering::Relaxed) >= initial_hits + 1);
        assert!(CACHE_MISSES.load(Ordering::Relaxed) >= initial_misses + 1);
    }

    struct QueueSourceImpl {}

    impl QueueSource for QueueSourceImpl {
        fn find_by_name(&mut self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(Some(Queue {
                id:                          1,
                name:                        name.to_string(),
                max_receives:                None,
                dead_letter_queue:           None,
                retention_timeout:           pg_interval(30),
                visibility_timeout:          pg_interval(30),
                message_delay:               pg_interval(30),
                content_based_deduplication: false,
                created_at:                  UtcTime::now(),
                updated_at:                  UtcTime::now(),
            }))
        }
    }
}
