use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::pg::data_types::PgInterval;

use crate::schema::queues;
use diesel::result::{Error, DatabaseErrorKind};

#[derive(Debug)]
pub struct QueueInput<'a> {
    pub name: &'a str,
    pub max_receives: Option<i32>,
    pub dead_letter_queue: &'a Option<String>,
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
    pub fn insert(conn: &PgConnection, queue: &QueueInput) -> QueryResult<bool> {
        let now = Utc::now();
        let result = diesel::dsl::insert_into(queues::table)
            .values(NewQueue {
                name: queue.name,
                max_receives: queue.max_receives,
                dead_letter_queue: match queue.dead_letter_queue {
                    None => None,
                    Some(s) => Some(&s),
                },
                retention_timeout: pg_interval(queue.retention_timeout),
                visibility_timeout: pg_interval(queue.visibility_timeout),
                message_delay: pg_interval(queue.message_delay),
                content_based_deduplication: queue.content_based_deduplication,
                created_at: now.naive_utc(),
                updated_at: now.naive_utc(),
            })
            .execute(conn);
        match result {
            Ok(_) => Ok(true),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

impl Queue {
    pub fn find_by_name(conn: &PgConnection, name: &str) -> QueryResult<Option<Queue>> {
        queues::table
            .filter(queues::name.eq(name))
            .first::<Queue>(conn)
            .optional()
    }
}
