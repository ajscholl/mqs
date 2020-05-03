use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::{
    backend::Backend,
    pg::{types::date_and_time::PgInterval, Pg},
    prelude::*,
    query_builder::{AstPass, QueryFragment},
    result::{DatabaseErrorKind, Error},
};
use sha2::{digest::Input, Digest, Sha256};
use std::ops::{Add, Deref};
use uuid::Uuid;

use crate::{
    models::{queue::Queue, PgRepository},
    schema::messages,
};

#[derive(Debug)]
pub struct MessageInput<'a> {
    pub payload:          &'a [u8],
    pub content_type:     &'a str,
    pub content_encoding: Option<&'a str>,
    pub trace_id:         Option<Uuid>,
}

#[derive(Insertable)]
#[table_name = "messages"]
pub struct NewMessage<'a> {
    pub id:               Uuid,
    pub payload:          &'a [u8],
    pub content_type:     &'a str,
    pub content_encoding: Option<&'a str>,
    pub hash:             Option<String>,
    pub queue:            &'a str,
    pub receives:         i32,
    pub visible_since:    NaiveDateTime,
    pub created_at:       NaiveDateTime,
    pub trace_id:         Option<Uuid>,
}

#[derive(Queryable, Associations, Identifiable, Serialize, Debug, Clone)]
pub struct Message {
    pub id:               Uuid,
    pub payload:          Vec<u8>,
    pub content_type:     String,
    pub content_encoding: Option<String>,
    pub hash:             Option<String>,
    pub queue:            String,
    pub receives:         i32,
    pub visible_since:    NaiveDateTime,
    pub created_at:       NaiveDateTime,
    pub trace_id:         Option<Uuid>,
}

pub(crate) fn add_pg_interval(time: &DateTime<Utc>, offset: &PgInterval) -> DateTime<Utc> {
    let us = Duration::microseconds(offset.microseconds);
    let d = Duration::days(offset.days as i64);
    let m = Duration::days(offset.months as i64 * 30);
    time.add(us + d + m)
}

pub trait MessageRepository: Send {
    fn insert_message(&self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool>;
    fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>>;
    fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize>;
    fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool>;
    fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize>;
}

impl MessageRepository for PgRepository {
    fn insert_message(&self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
        let now = Utc::now();
        let visible_since = add_pg_interval(&now, &queue.message_delay);
        let id = Uuid::new_v4();
        let hash = if queue.content_based_deduplication {
            let mut digest = Sha256::default();
            Input::input(&mut digest, input.payload);
            let result = digest.result();
            Some(base64::encode(result.as_slice()))
        } else {
            None
        };
        let result = diesel::dsl::insert_into(messages::table)
            .values(NewMessage {
                id,
                payload: input.payload,
                content_type: input.content_type,
                content_encoding: input.content_encoding,
                hash,
                queue: &queue.name,
                receives: 0,
                visible_since: visible_since.naive_utc(),
                created_at: now.naive_utc(),
                trace_id: input.trace_id,
            })
            .execute(self.conn.deref());
        match result {
            Ok(_) => Ok(true),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
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

        let messages: Vec<Message> = update_query.get_results(self.conn.deref())?;

        // filter result, move messages to dead letter queues
        let mut result = Vec::with_capacity(messages.len());
        let mut move_to_dead_letter_queue = Vec::new();
        let mut to_delete = Vec::new();
        for message in messages {
            let created_at = DateTime::from_utc(message.created_at, Utc);
            if add_pg_interval(&created_at, &queue.retention_timeout) < now {
                to_delete.push(message.id);
                continue;
            }
            if let Some(max_receives) = queue.max_receives {
                if message.receives >= max_receives {
                    // send to dead letter queue
                    move_to_dead_letter_queue.push(message.id);
                    // do not put a continue statement here, we still want to return this message
                    // to the caller. So we send a message directly to the dead-letter-queue upon receive,
                    // but we still allow the caller to process it. It will appear in the dead-letter-queue
                    // after the visibility timeout and the caller will still be able to delete it via
                    // its id regardless of the queue the message is in
                }
            }
            result.push(message);
        }
        if !to_delete.is_empty() {
            self.delete_messages_by_ids(to_delete)?;
        }
        if let Some(dead_letter_queue) = &queue.dead_letter_queue {
            if !move_to_dead_letter_queue.is_empty() {
                self.move_message_to_queue(move_to_dead_letter_queue, dead_letter_queue)?;
            }
        }
        return Ok(result);
    }

    fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
        diesel::dsl::update(messages::table)
            .set((messages::queue.eq(new_queue), messages::receives.eq(0)))
            .filter(messages::id.eq_any(ids))
            .execute(self.conn.deref())
    }

    fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool> {
        diesel::delete(messages::table.filter(messages::id.eq(id)))
            .execute(self.conn.deref())
            .map(|count| count > 0)
    }

    fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize> {
        diesel::delete(messages::table.filter(messages::id.eq_any(ids))).execute(self.conn.deref())
    }
}

struct MessageIdsForFetch<'a> {
    queue_name:    &'a str,
    visible_since: NaiveDateTime,
    count:         i64,
}

impl<'a> MessageIdsForFetch<'a> {
    fn new(queue_name: &'a str, visible_since: NaiveDateTime, count: i64) -> MessageIdsForFetch<'a> {
        MessageIdsForFetch {
            queue_name,
            visible_since,
            count,
        }
    }
}

impl<'a> QueryFragment<Pg> for MessageIdsForFetch<'a> {
    fn walk_ast(&self, mut out: AstPass<'_, Pg>) -> QueryResult<()> {
        // select all elements which are currently visible, take the first elements visible
        // and limit to the maximum number of elements we want to process.
        // skip any locked elements and lock our elements for update.
        let sub_query = messages::table
            .select(messages::id)
            .filter(
                messages::queue
                    .eq(self.queue_name)
                    .and(messages::visible_since.le(self.visible_since)),
            )
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

impl<'a> Expression for MessageIdsForFetch<'a> {
    type SqlType = <messages::columns::id as Expression>::SqlType;
}

impl<'a> AppearsOnTable<messages::table> for MessageIdsForFetch<'a> {}

struct In<F, V> {
    field:  F,
    values: V,
}

impl<F, V, T> Expression for In<F, V>
where
    F: Expression<SqlType = T>,
    V: Expression<SqlType = T>,
{
    type SqlType = diesel::sql_types::Bool;
}

impl<F, V> In<F, V> {
    fn new(field: F, values: V) -> In<F, V> {
        In { field, values }
    }
}

impl<F, V, DB> QueryFragment<DB> for In<F, V>
where
    DB: Backend,
    F: QueryFragment<DB>,
    V: QueryFragment<DB>,
{
    fn walk_ast(&self, mut out: AstPass<'_, DB>) -> QueryResult<()> {
        self.field.walk_ast(out.reborrow())?;
        out.push_sql(" IN ");
        self.values.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl<F, V, T, Table> AppearsOnTable<Table> for In<F, V>
where
    F: Expression<SqlType = T> + AppearsOnTable<Table>,
    V: Expression<SqlType = T> + AppearsOnTable<Table>,
{
}
