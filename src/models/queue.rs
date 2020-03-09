use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::pg::data_types::PgInterval;
use cached::stores::TimedCache;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schema::messages;
use crate::schema::queues;
use diesel::result::{Error, DatabaseErrorKind};
use cached::once_cell::sync::Lazy;
use std::sync::Mutex;
use cached::Cached;

#[derive(Debug)]
pub struct QueueInput<'a> {
    pub name: &'a str,
    pub max_receives: Option<i32>,
    pub dead_letter_queue: Option<&'a str>,
    pub retention_timeout: i64,
    pub visibility_timeout: i64,
    pub message_delay: i64,
    pub content_based_deduplication: bool,
}

#[derive(Insertable)]
#[table_name="queues"]
pub struct NewQueue<'a> {
    pub name: &'a str,
    pub max_receives: Option<i32>,
    pub dead_letter_queue: Option<&'a str>,
    pub retention_timeout: PgInterval,
    pub visibility_timeout: PgInterval,
    pub message_delay: PgInterval,
    pub content_based_deduplication: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Queryable, Associations, Identifiable, Clone, Debug, PartialEq)]
pub struct Queue {
    pub id: i32,
    pub name: String,
    pub max_receives: Option<i32>,
    pub dead_letter_queue: Option<String>,
    pub retention_timeout: PgInterval,
    pub visibility_timeout: PgInterval,
    pub message_delay: PgInterval,
    pub content_based_deduplication: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

fn pg_interval(mut seconds: i64) -> PgInterval {
    if seconds < 0 {
        let int = pg_interval(-seconds);

        return PgInterval {
            microseconds: -int.microseconds,
            days: -int.days,
            months: -int.months,
        };
    }

    let mut days = seconds / (24 * 3600);
    seconds -= days * (24 * 3600);
    let months = days / 30;
    days -= months * 30;

    PgInterval {
        microseconds: seconds * 1_000_000,
        days: days as i32,
        months: months as i32,
    }
}

pub struct QueueDescription {
    pub queue: Queue,
    pub messages: i64,
    pub visible_messages: i64,
    pub oldest_message_age: i64,
}

pub trait QueueSource: Sized {
    fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>>;
    fn find_by_name_cached(&self, name: &str) -> QueryResult<Option<Queue>> {
        find_queue_by_name_cached(self, name)
    }
}

pub trait QueueRepository: QueueSource {
    fn insert(&self, queue: &QueueInput) -> QueryResult<Option<Queue>>;
    fn count(&self) -> QueryResult<i64>;
    fn describe(&self, name: &str) -> QueryResult<Option<QueueDescription>>;
    fn list(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>>;
    fn update(&self, queue: &QueueInput) -> QueryResult<Option<Queue>>;
    fn delete_by_name(&self, name: &str) -> QueryResult<Option<Queue>>;
}

pub struct PgQueueRepository<'a> {
    conn: &'a PgConnection,
}

impl <'a> PgQueueRepository<'a> {
    pub fn new(conn: &'a PgConnection) -> Self {
        PgQueueRepository { conn }
    }
}

impl <'a> QueueSource for PgQueueRepository<'a> {
    fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
        queues::table
            .filter(queues::name.eq(name))
            .first::<Queue>(self.conn)
            .optional()
    }
}

impl <'a> QueueRepository for PgQueueRepository<'a> {
    fn insert(&self, queue: &QueueInput) -> QueryResult<Option<Queue>> {
        let now = Utc::now();
        let result = diesel::dsl::insert_into(queues::table)
            .values(NewQueue {
                name: queue.name,
                max_receives: queue.max_receives,
                dead_letter_queue: match queue.dead_letter_queue {
                    None => None,
                    Some(s) => Some(s),
                },
                retention_timeout: pg_interval(queue.retention_timeout),
                visibility_timeout: pg_interval(queue.visibility_timeout),
                message_delay: pg_interval(queue.message_delay),
                content_based_deduplication: queue.content_based_deduplication,
                created_at: now.naive_utc(),
                updated_at: now.naive_utc(),
            })
            .returning(queues::all_columns)
            .get_result(self.conn);
        match result {
            Ok(queue) => Ok(Some(queue)),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn count(&self) -> QueryResult<i64> {
        queues::table
            .count()
            .get_result(self.conn)
    }

    fn describe(&self, name: &str) -> QueryResult<Option<QueueDescription>> {
        match self.find_by_name(name)? {
            None => Ok(None),
            Some(queue) => {
                let messages = messages::table
                    .filter(messages::queue.eq(&queue.name))
                    .count()
                    .get_result(self.conn)?;
                let now = Utc::now();
                let visible_messages = messages::table
                    .filter(messages::queue.eq(&queue.name).and(messages::visible_since.le(now.naive_utc())))
                    .count()
                    .get_result(self.conn)?;
                let oldest_message: Option<NaiveDateTime> = messages::table
                    .select(messages::created_at)
                    .filter(messages::queue.eq(&queue.name))
                    .limit(1)
                    .order(messages::created_at.asc())
                    .for_key_share()
                    .skip_locked()
                    .get_result(self.conn)
                    .optional()?;

                Ok(Some(QueueDescription {
                    queue,
                    messages,
                    visible_messages,
                    oldest_message_age: match oldest_message {
                        None => 0,
                        Some(created_at) => now.naive_utc().timestamp() - created_at.timestamp(),
                    },
                }))
            }
        }
    }

    fn list(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
        let query = queues::table
            .order(queues::id.asc());

        match offset {
            None => match limit {
                None => query.get_results(self.conn),
                Some(limit) => query.limit(limit).get_results(self.conn),
            },
            Some(offset) => match limit {
                None => query.offset(offset).get_results(self.conn),
                Some(limit) => query.offset(offset).limit(limit).get_results(self.conn),
            },
        }
    }

    fn update(&self, queue: &QueueInput) -> QueryResult<Option<Queue>> {
        diesel::dsl::update(queues::table.filter(queues::name.eq(queue.name)))
            .set((
                queues::max_receives.eq(queue.max_receives),
                queues::dead_letter_queue.eq(match queue.dead_letter_queue {
                    None => None,
                    Some(s) => Some(s),
                }),
                queues::retention_timeout.eq(pg_interval(queue.retention_timeout)),
                queues::visibility_timeout.eq(pg_interval(queue.visibility_timeout)),
                queues::message_delay.eq(pg_interval(queue.message_delay)),
                queues::content_based_deduplication.eq(queue.content_based_deduplication),
                queues::updated_at.eq(Utc::now().naive_utc()),
            ))
            .returning(queues::all_columns)
            .get_result(self.conn)
            .optional()
    }

    fn delete_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
        diesel::dsl::delete(queues::table.filter(queues::name.eq(name)))
            .returning(queues::all_columns)
            .get_result(self.conn)
            .optional()
    }
}

static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
static QUEUE_CACHE: Lazy<Mutex<TimedCache<String, Queue>>> = Lazy::new(|| Mutex::new(TimedCache::with_lifespan(10)));

fn find_queue_by_name_cached<R: QueueSource>(repo: &R, queue_name: &str) -> QueryResult<Option<Queue>> {
    let key = queue_name.to_string();
    if let Ok(mut cache) = QUEUE_CACHE.lock() {
        let res = Cached::cache_get(&mut *cache, &key);
        if let Some(cached_val) = res {
            let cache_hits = CACHE_HITS.fetch_add(1, Ordering::Relaxed) + 1;
            info!("Found queue of name {}, total {} cache hits", &cached_val.name, cache_hits);
            return Ok(Some(cached_val.clone()));
        }
    } else {
        error!("Failed to get queue cache lock");
        return repo.find_by_name(queue_name);
    }
    let cache_misses = CACHE_MISSES.fetch_add(1, Ordering::Relaxed) + 1;
    info!("Getting queue of name {}, total {} cache misses", queue_name, cache_misses);
    match repo.find_by_name(queue_name) {
        Err(e) => Err(e),
        Ok(None) => Ok(None),
        Ok(Some(queue)) => {
            if let Ok(mut cache) = QUEUE_CACHE.lock() {
                Cached::cache_set(&mut *cache, key, queue.clone());
            } else {
                error!("Failed to get queue cache lock");
            }
            Ok(Some(queue))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn cache_test() {
        let repo = QueueSourceImpl {};
        let queue = repo.find_by_name_cached("my queue").unwrap().unwrap();
        assert_eq!(CACHE_HITS.load(Ordering::Relaxed), 0);
        assert_eq!(CACHE_MISSES.load(Ordering::Relaxed), 1);
        let cached = repo.find_by_name_cached("my queue").unwrap().unwrap();
        assert_eq!(queue, cached);
        assert_eq!(CACHE_HITS.load(Ordering::Relaxed), 1);
        assert_eq!(CACHE_MISSES.load(Ordering::Relaxed), 1);
    }

    struct QueueSourceImpl {}

    impl QueueSource for QueueSourceImpl {
        fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            Ok(Some(Queue {
                id: 1,
                name: name.to_string(),
                max_receives: None,
                dead_letter_queue: None,
                retention_timeout: pg_interval(30),
                visibility_timeout: pg_interval(30),
                message_delay: pg_interval(30),
                content_based_deduplication: false,
                created_at: Utc::now().naive_utc(),
                updated_at: Utc::now().naive_utc(),
            }))
        }
    }
}
