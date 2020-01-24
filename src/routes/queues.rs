use rocket::request::{FromRequest, Outcome};
use rocket::Request;
use rocket::http::{RawStr, Status};
use rocket_contrib::json::{Json, JsonError};
use diesel::pg::types::date_and_time::PgInterval;
use diesel::QueryResult;

use crate::connection::DbConn;
use crate::models::queue::{NewQueue, QueueInput, Queue};
use crate::routes::{ErrorResponder, StatusResponder, JsonResponder};

#[derive(Serialize, Deserialize, Debug)]
struct QueueRedrivePolicy {
    max_receives: i32,
    dead_letter_queue: String,
}

#[derive(Deserialize, Debug)]
pub struct QueueConfig {
    redrive_policy: Option<QueueRedrivePolicy>,
    retention_timeout: i64,
    visibility_timeout: i64,
    message_delay: i64,
    message_deduplication: bool,
}

#[derive(Serialize, Debug)]
pub struct QueueConfigOutput {
    name: String,
    redrive_policy: Option<QueueRedrivePolicy>,
    retention_timeout: i64,
    visibility_timeout: i64,
    message_delay: i64,
    message_deduplication: bool,
}

impl QueueConfigOutput {
    fn new(queue: Queue) -> QueueConfigOutput {
        QueueConfigOutput {
            name: queue.name,
            redrive_policy: match (queue.dead_letter_queue, queue.max_receives) {
                (Some(dead_letter_queue), Some(max_receives)) => Some(QueueRedrivePolicy {
                    max_receives,
                    dead_letter_queue,
                }),
                _ => None,
            },
            retention_timeout: pg_interval_seconds(&queue.retention_timeout),
            visibility_timeout: pg_interval_seconds(&queue.visibility_timeout),
            message_delay: pg_interval_seconds(&queue.message_delay),
            message_deduplication: queue.content_based_deduplication,
        }
    }
}

impl QueueConfig {
    fn to_input<'a>(&'a self, queue_name: &'a str) -> QueueInput<'a> {
        QueueInput {
                name: queue_name,
                max_receives: match &self.redrive_policy {
                    None => None,
                    Some(p) => Some(p.max_receives),
                },
                dead_letter_queue: match &self.redrive_policy {
                    None => None,
                    Some(p) => Some(&p.dead_letter_queue),
                },
                retention_timeout: self.retention_timeout,
                visibility_timeout: self.visibility_timeout,
                message_delay: self.message_delay,
                content_based_deduplication: self.message_deduplication,
            }
    }
}

#[put("/queues/<queue_name>", data = "<params>", format = "application/json")]
pub fn new_queue(conn: DbConn, queue_name: String, params: Result<Json<QueueConfig>, JsonError>) -> Result<JsonResponder<QueueConfigOutput>, Result<StatusResponder, ErrorResponder>> {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            Err(Err(ErrorResponder::new_owned(err_message)))
        },
        Ok(Json(config)) => {
            info!("Creating new queue {}", &queue_name);
            let created = NewQueue::insert(&conn, &config.to_input(&queue_name));

            match created {
                Ok(Some(queue)) => {
                    info!("Created new queue {}", &queue_name);
                    Ok(JsonResponder::new(Status::Created, QueueConfigOutput::new(queue)))
                },
                Ok(None) => {
                    info!("Queue {} did already exist", &queue_name);
                    Err(Ok(StatusResponder::new(Status::Conflict)))
                },
                Err(err) => {
                    error!("Failed to create new queue {}, {:?}: {}", queue_name, config, err);
                    Err(Ok(StatusResponder::new(Status::InternalServerError)))
                }
            }
        }
    }
}

#[post("/queues/<queue_name>", data = "<params>", format = "application/json")]
pub fn update_queue(conn: DbConn, queue_name: String, params: Result<Json<QueueConfig>, JsonError>) -> Result<JsonResponder<QueueConfigOutput>, Result<StatusResponder, ErrorResponder>> {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            Err(Err(ErrorResponder::new_owned(err_message)))
        },
        Ok(Json(config)) => {
            info!("Updating queue {}", &queue_name);
            let result = Queue::update(&conn, &config.to_input(&queue_name));

            match result {
                Ok(Some(queue)) => {
                    info!("Updated queue {}", &queue_name);
                    Ok(JsonResponder::new(Status::Ok, QueueConfigOutput::new(queue)))
                },
                Ok(None) => {
                    info!("Queue {} did not exist", &queue_name);
                    Err(Ok(StatusResponder::new(Status::NotFound)))
                },
                Err(err) => {
                    error!("Failed to update queue {}, {:?}: {}", queue_name, config, err);
                    Err(Ok(StatusResponder::new(Status::InternalServerError)))
                }
            }
        }
    }
}

#[delete("/queues/<queue_name>")]
pub fn delete_queue(conn: DbConn, queue_name: String) -> Result<JsonResponder<QueueConfigOutput>, StatusResponder> {
    info!("Deleting queue {}", &queue_name);
    let deleted = Queue::delete_by_name(&conn, &queue_name);
    match deleted {
        Ok(Some(queue)) => {
            info!("Deleted queue {}", &queue_name);
            Ok(JsonResponder::new(Status::Ok, QueueConfigOutput::new(queue)))
        },
        Ok(None) => {
            info!("Message {} was not found", &queue_name);
            Err(StatusResponder::new(Status::NotFound))
        },
        Err(err) => {
            error!("Failed to delete queue {}: {}", &queue_name, err);
            Err(StatusResponder::new(Status::InternalServerError))
        },
    }
}

#[derive(Debug)]
pub struct QueuesRange {
    offset: Option<i64>,
    limit: Option<i64>,
}

fn check_option<T, E>(value: Option<Result<T, E>>) -> Result<Option<T>, E> {
    match value {
        None => Ok(None),
        Some(Ok(val)) => Ok(Some(val)),
        Some(Err(err)) => Err(err),
    }
}

impl <'a, 'r> FromRequest<'a, 'r> for QueuesRange {
    type Error = String;

    fn from_request(request: &'a Request<'r>) -> Outcome<Self, <Self as FromRequest<'a, 'r>>::Error> {
        let offset = check_option::<i64, &RawStr>(request.get_query_value("offset"));
        let limit = check_option::<i64, &RawStr>(request.get_query_value("limit"));

        match (offset, limit) {
            (Err(err), _) => Outcome::Failure((Status::BadRequest, format!("invalid value for number field offset: {}", err))),
            (_, Err(err)) => Outcome::Failure((Status::BadRequest, format!("invalid value for number field limit: {}", err))),
            (Ok(offset), Ok(limit)) => Outcome::Success(QueuesRange {
                offset,
                limit,
            })
        }
    }
}

fn pg_interval_seconds(interval: &PgInterval) -> i64 {
    interval.microseconds / 1000000
        + interval.days as i64 * (24 * 3600)
        + interval.months as i64 * (30 * 24 * 3600)
}

#[derive(Serialize, Debug)]
pub struct QueuesResponse {
    queues: Vec<QueueConfigOutput>,
    total: i64,
}

fn list_queues_and_count(conn: DbConn, range: &QueuesRange) -> QueryResult<QueuesResponse> {
    let queues = Queue::list(&conn, range.offset, range.limit)?;
    let total = Queue::count(&conn)?;
    Ok(QueuesResponse {
        queues: queues.into_iter().map(|queue| QueueConfigOutput::new(queue)).collect(),
        total,
    })
}

#[get("/queues")]
pub fn list_queues(conn: DbConn, range: Result<QueuesRange, String>) -> Result<Result<JsonResponder<QueuesResponse>, ErrorResponder>, StatusResponder> {
    match range {
        Err(err) => Ok(Err(ErrorResponder::new_owned(err))),
        Ok(range) => match list_queues_and_count(conn, &range) {
            Ok(response) => Ok(Ok(JsonResponder::new(Status::Ok, response))),
            Err(err) => {
                error!("Failed to read range of queues {:?}-{:?}: {}", range.offset, range.limit, err);

                Err(StatusResponder::new(Status::InternalServerError))
            },
        },
    }
}

#[derive(Serialize, Debug)]
pub struct QueueStatus {
    messages: i64,
    visible_messages: i64,
    oldest_message_age: i64,
}

#[derive(Serialize, Debug)]
pub struct QueueDescription {
    name: String,
    redrive_policy: Option<QueueRedrivePolicy>,
    retention_timeout: i64,
    visibility_timeout: i64,
    message_delay: i64,
    message_deduplication: bool,
    status: QueueStatus,
}

impl QueueDescription {
    fn new(queue: Queue, messages: i64, visible_messages: i64, oldest_message_age: i64) -> QueueDescription {
        let queue_description = QueueConfigOutput::new(queue);

        QueueDescription {
            name: queue_description.name,
            redrive_policy: queue_description.redrive_policy,
            retention_timeout: queue_description.retention_timeout,
            visibility_timeout: queue_description.visibility_timeout,
            message_delay: queue_description.message_delay,
            message_deduplication: queue_description.message_deduplication,
            status: QueueStatus {
                messages,
                visible_messages,
                oldest_message_age,
            },
        }
    }
}

#[get("/queues/<queue_name>")]
pub fn describe_queue(conn: DbConn, queue_name: String) -> Result<JsonResponder<QueueDescription>, StatusResponder> {
    match Queue::describe(&conn, &queue_name) {
        Err(err) => {
            error!("Failed to describe queue {}: {}", &queue_name, err);
            Err(StatusResponder::new(Status::InternalServerError))
        },
        Ok(None) => Err(StatusResponder::new(Status::NotFound)),
        Ok(Some(description)) => Ok(JsonResponder::new(Status::Ok, QueueDescription::new(description.queue, description.messages, description.visible_messages, description.oldest_message_age))),
    }
}
