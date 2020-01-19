use chrono::{NaiveDateTime, Utc, DateTime};
use diesel::prelude::*;
use diesel::pg::{PgConnection, Pg};
use diesel::pg::types::date_and_time::PgInterval;
use diesel::result::{Error, DatabaseErrorKind};
use diesel::query_builder::{QueryFragment, AstPass};
use diesel::backend::Backend;
use time::Duration;
use uuid::Uuid;
use sha2::{Sha256, Digest};
use sha2::digest::Input;
use std::ops::Add;

use crate::schema::messages;
use crate::models::queue::Queue;

#[derive(Debug)]
pub struct MessageInput<'a> {
    pub payload: &'a str,
    pub content_type: &'a str,
}

#[derive(Insertable)]
#[table_name="messages"]
pub struct NewMessage<'a> {
    pub id: Uuid,
    pub payload: &'a str,
    pub content_type: &'a str,
    pub hash: Option<String>,
    pub queue: &'a str,
    pub receives: i32,
    pub visible_since: NaiveDateTime,
    pub created_at: NaiveDateTime,
}

#[derive(Queryable, Associations, Identifiable, Serialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: String,
    pub content_type: String,
    pub hash: Option<String>,
    pub queue: String,
    pub receives: i32,
    pub visible_since: NaiveDateTime,
    pub created_at: NaiveDateTime,
}

fn add_pg_interval(time: &DateTime<Utc>, offset: &PgInterval) -> DateTime<Utc> {
    let us = Duration::microseconds(offset.microseconds);
    let d = Duration::days(offset.days as i64);
    let m = Duration::days(offset.months as i64 * 30);
    time.add(us + d + m)
}

impl <'a> NewMessage<'a> {
    pub fn insert(conn: &PgConnection, queue: &Queue, input: &MessageInput) -> QueryResult<bool> {
        let now = Utc::now();
        let visible_since = add_pg_interval(&now, &queue.message_delay);
        let id = Uuid::new_v4();
        let hash = if queue.content_based_deduplication {
            let mut digest = Sha256::default();
            Input::input(&mut digest, input.payload.as_bytes());
            let result = digest.result();
            Some(base64::encode(result.as_slice()))
        } else { None };
        let result = diesel::dsl::insert_into(messages::table)
            .values(NewMessage {
                id,
                payload: input.payload,
                content_type: input.content_type,
                hash,
                queue: &queue.name,
                receives: 0,
                visible_since: visible_since.naive_utc(),
                created_at: now.naive_utc(),
            })
            .execute(conn);
        match result {
            Ok(_) => Ok(true),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

struct MessageIdsForFetch<'a> {
    queue_name: &'a str,
    visible_since: NaiveDateTime,
    count: i64,
}

impl <'a> MessageIdsForFetch<'a> {
    fn new(queue_name: &'a str, visible_since: NaiveDateTime, count: i64) -> MessageIdsForFetch<'a> {
        MessageIdsForFetch {
            queue_name,
            visible_since,
            count,
        }
    }
}

impl <'a> QueryFragment<Pg> for MessageIdsForFetch<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        // select all elements which are currently visible, take the first elements visible
        // and limit to the maximum number of elements we want to process.
        // skip any locked elements and lock our elements for update.
        let sub_query = messages::table
            .select(messages::id)
            .filter(messages::queue.eq(self.queue_name).and(messages::visible_since.le(self.visible_since)))
            .order(messages::visible_since.asc())
            .for_update()
            .skip_locked()
            .limit(self.count);
        out.push_sql("(");
        sub_query.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

impl <'a> Expression for MessageIdsForFetch<'a> {
    type SqlType = <messages::columns::id as Expression>::SqlType;
}

impl <'a> AppearsOnTable<messages::table> for MessageIdsForFetch<'a> {}

struct In<F, V> {
    field: F,
    values: V,
}

impl <F, V, T> Expression for In<F, V>
    where F: Expression<SqlType=T>, V: Expression<SqlType=T> {
    type SqlType = diesel::sql_types::Bool;
}

impl <F, V> In<F, V> {
    fn new(field: F, values: V) -> In<F, V> {
        In {
            field,
            values,
        }
    }
}

impl <F, V, DB> QueryFragment<DB> for In<F, V>
    where DB: Backend, F: QueryFragment<DB>, V: QueryFragment<DB> {
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        self.field.walk_ast(out.reborrow())?;
        out.push_sql(" IN ");
        self.values.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl <F, V, T, Table> AppearsOnTable<Table> for In<F, V>
    where F: Expression<SqlType=T> + AppearsOnTable<Table>, V: Expression<SqlType=T> + AppearsOnTable<Table> {}

impl Message {
    pub fn get_from_queue(conn: &PgConnection, queue: &Queue, count: i64)-> QueryResult<Vec<Message>> {
        let now = Utc::now();
        let visible_since = add_pg_interval(&now, &queue.visibility_timeout);

        let update_query = diesel::dsl::update(messages::table)
            .set((
                     messages::visible_since.eq(visible_since.naive_utc()),
                     messages::receives.eq(messages::receives + 1),
            ))
            .filter(In::new(
                messages::id,
                MessageIdsForFetch::new(&queue.name, now.naive_utc(), count),
            ))
            .returning(messages::all_columns);

        return update_query.get_results(conn);
    }

    pub fn delete_by_id(conn: &PgConnection, id: Uuid) -> QueryResult<bool> {
        diesel::delete(messages::table.filter(messages::id.eq(id)))
            .execute(conn).map(|count| count > 0)
    }
}
