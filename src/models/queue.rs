use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::pg::data_types::PgInterval;

use crate::schema::messages;
use crate::schema::queues;
use diesel::result::{Error, DatabaseErrorKind};

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

#[derive(Queryable, Associations, Identifiable)]
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

impl <'a> NewQueue<'a> {
    pub fn insert(conn: &PgConnection, queue: &QueueInput) -> QueryResult<Option<Queue>> {
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
            .get_result(conn);
        match result {
            Ok(queue) => Ok(Some(queue)),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub struct QueueDescription {
    pub queue: Queue,
    pub messages: i64,
    pub visible_messages: i64,
    pub oldest_message_age: i64,
}

impl Queue {
    pub fn find_by_name(conn: &PgConnection, name: &str) -> QueryResult<Option<Queue>> {
        queues::table
            .filter(queues::name.eq(name))
            .first::<Queue>(conn)
            .optional()
    }

    pub fn count(conn: &PgConnection) -> QueryResult<i64> {
        queues::table
            .count()
            .get_result(conn)
    }

    pub fn describe(conn: &PgConnection, name: &str) -> QueryResult<Option<QueueDescription>> {
        match Queue::find_by_name(conn, name)? {
            None => Ok(None),
            Some(queue) => {
                let messages = messages::table
                    .filter(messages::queue.eq(&queue.name))
                    .count()
                    .get_result(conn)?;
                let now = Utc::now();
                let visible_messages = messages::table
                    .filter(messages::queue.eq(&queue.name).and(messages::visible_since.le(now.naive_utc())))
                    .count()
                    .get_result(conn)?;
                let oldest_message: Option<NaiveDateTime> = messages::table
                    .select(messages::created_at)
                    .filter(messages::queue.eq(&queue.name))
                    .limit(1)
                    .order(messages::created_at.asc())
                    .for_key_share()
                    .skip_locked()
                    .get_result(conn)
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

    pub fn list(conn: &PgConnection, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
        let query = queues::table
            .order(queues::id.asc());

        match offset {
            None => match limit {
                None => query.get_results(conn),
                Some(limit) => query.limit(limit).get_results(conn),
            },
            Some(offset) => match limit {
                None => query.offset(offset).get_results(conn),
                Some(limit) => query.offset(offset).limit(limit).get_results(conn),
            },
        }
    }

    pub fn update(conn: &PgConnection, queue: &QueueInput) -> QueryResult<Option<Queue>> {
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
            .get_result(conn)
            .optional()
    }

    pub fn delete_by_name(conn: &PgConnection, name: &str) -> QueryResult<Option<Queue>> {
        diesel::dsl::delete(queues::table.filter(queues::name.eq(name)))
            .returning(queues::all_columns)
            .get_result(conn)
            .optional()
    }
}
