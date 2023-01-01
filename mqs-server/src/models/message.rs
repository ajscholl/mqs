use diesel::{
    backend::Backend,
    pg::Pg,
    prelude::*,
    query_builder::{AstPass, QueryFragment},
    result::{DatabaseErrorKind, Error},
};
use mqs_common::UtcTime;
use sha2::{Digest, Sha256};
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
#[diesel(table_name = messages)]
pub struct NewMessage<'a> {
    pub id:               Uuid,
    pub payload:          &'a [u8],
    pub content_type:     &'a str,
    pub content_encoding: Option<&'a str>,
    pub hash:             Option<String>,
    pub queue:            &'a str,
    pub receives:         i32,
    pub visible_since:    UtcTime,
    pub created_at:       UtcTime,
    pub trace_id:         Option<Uuid>,
}

#[derive(Queryable, Identifiable, Serialize, Debug, Clone)]
pub struct Message {
    pub id:               Uuid,
    pub payload:          Vec<u8>,
    pub content_type:     String,
    pub content_encoding: Option<String>,
    pub hash:             Option<String>,
    pub queue:            String,
    pub receives:         i32,
    pub visible_since:    UtcTime,
    pub created_at:       UtcTime,
    pub trace_id:         Option<Uuid>,
}

pub trait MessageRepository: Send {
    fn insert_message(&mut self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool>;
    fn get_message_from_queue(&mut self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>>;
    fn move_message_to_queue(&mut self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize>;
    fn delete_message_by_id(&mut self, id: Uuid) -> QueryResult<bool>;
    fn delete_messages_by_ids(&mut self, ids: Vec<Uuid>) -> QueryResult<usize>;
}

impl MessageRepository for PgRepository {
    fn insert_message(&mut self, queue: &Queue, input: &MessageInput<'_>) -> QueryResult<bool> {
        let now = UtcTime::now();
        let visible_since = now.add_pg_interval(&queue.message_delay);
        let id = Uuid::new_v4();
        let hash = if queue.content_based_deduplication {
            let mut digest = Sha256::default();
            digest.update(input.payload);
            let result = digest.finalize();
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
                visible_since,
                created_at: now,
                trace_id: input.trace_id,
            })
            .execute(&mut self.conn);
        match result {
            Ok(_) => Ok(true),
            Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn get_message_from_queue(&mut self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
        let now = UtcTime::now();
        let visible_since = now.add_pg_interval(&queue.visibility_timeout);

        let update_query = diesel::dsl::update(messages::table)
            .set((
                messages::visible_since.eq(visible_since),
                messages::receives.eq(messages::receives + 1),
            ))
            .filter(In::new(messages::id, MessageIdsForFetch::new(&queue.name, now, count)))
            .returning(messages::all_columns);

        let messages: Vec<Message> = update_query.get_results(&mut self.conn)?;

        // filter result, move messages to dead letter queues
        let mut result = Vec::with_capacity(messages.len());
        let mut move_to_dead_letter_queue = Vec::new();
        let mut to_delete = Vec::new();
        for message in messages {
            if message.created_at.add_pg_interval(&queue.retention_timeout) < now {
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
        Ok(result)
    }

    fn move_message_to_queue(&mut self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
        diesel::dsl::update(messages::table)
            .set((messages::queue.eq(new_queue), messages::receives.eq(0)))
            .filter(messages::id.eq_any(ids))
            .execute(&mut self.conn)
    }

    fn delete_message_by_id(&mut self, id: Uuid) -> QueryResult<bool> {
        diesel::delete(messages::table.filter(messages::id.eq(id)))
            .execute(&mut self.conn)
            .map(|count| count > 0)
    }

    fn delete_messages_by_ids(&mut self, ids: Vec<Uuid>) -> QueryResult<usize> {
        diesel::delete(messages::table.filter(messages::id.eq_any(ids))).execute(&mut self.conn)
    }
}

struct MessageIdsForFetch {
    sub_query: Box<dyn QueryFragment<Pg>>,
}

impl MessageIdsForFetch {
    fn new(queue_name: &str, visible_since: UtcTime, count: i64) -> Self {
        Self {
            // select all elements which are currently visible, take the first elements visible
            // and limit to the maximum number of elements we want to process.
            // skip any locked elements and lock our elements for update.
            sub_query: Box::new(
                messages::table
                    .select(messages::id)
                    .filter(
                        messages::queue
                            .eq(queue_name.to_string())
                            .and(messages::visible_since.le(visible_since)),
                    )
                    .order(messages::visible_since.asc())
                    .for_update()
                    .skip_locked()
                    .limit(count),
            ),
        }
    }
}

impl QueryFragment<Pg> for MessageIdsForFetch {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("(");
        self.sub_query.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

impl Expression for MessageIdsForFetch {
    type SqlType = <messages::columns::id as Expression>::SqlType;
}

impl AppearsOnTable<messages::table> for MessageIdsForFetch {}

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
    const fn new(field: F, values: V) -> Self {
        Self { field, values }
    }
}

impl<F, V, DB> QueryFragment<DB> for In<F, V>
where
    DB: Backend,
    F: QueryFragment<DB>,
    V: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
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
