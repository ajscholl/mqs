use std::collections::HashMap;
use hyper::Body;
use diesel::pg::types::date_and_time::PgInterval;
use diesel::QueryResult;

use crate::connection::DbConn;
use crate::models::queue::{NewQueue, QueueInput, Queue};
use crate::routes::MqsResponse;
use crate::status::Status;

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueRedrivePolicy {
    pub max_receives: i32,
    pub dead_letter_queue: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueConfig {
    pub redrive_policy: Option<QueueRedrivePolicy>,
    pub retention_timeout: i64,
    pub visibility_timeout: i64,
    pub message_delay: i64,
    pub message_deduplication: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueConfigOutput {
    pub name: String,
    pub redrive_policy: Option<QueueRedrivePolicy>,
    pub retention_timeout: i64,
    pub visibility_timeout: i64,
    pub message_delay: i64,
    pub message_deduplication: bool,
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

pub fn new_queue(conn: DbConn, queue_name: &str, params: Result<QueueConfig, serde_json::Error>) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(err_message)
        },
        Ok(config) => {
            info!("Creating new queue {}", queue_name);
            let created = NewQueue::insert(&conn, &config.to_input(queue_name));

            match created {
                Ok(Some(queue)) => {
                    info!("Created new queue {}", queue_name);
                    MqsResponse::status_json(Status::Created, &QueueConfigOutput::new(queue))
                },
                Ok(None) => {
                    info!("Queue {} did already exist", queue_name);
                    MqsResponse::status(Status::Conflict)
                },
                Err(err) => {
                    error!("Failed to create new queue {}, {:?}: {}", queue_name, config, err);
                    MqsResponse::status(Status::InternalServerError)
                }
            }
        }
    }
}

pub fn update_queue(conn: DbConn, queue_name: &str, params: Result<QueueConfig, serde_json::Error>) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(err_message)
        },
        Ok(config) => {
            info!("Updating queue {}", queue_name);
            let result = Queue::update(&conn, &config.to_input(queue_name));

            match result {
                Ok(Some(queue)) => {
                    info!("Updated queue {}", queue_name);
                    MqsResponse::json(&QueueConfigOutput::new(queue))
                },
                Ok(None) => {
                    info!("Queue {} did not exist", queue_name);
                    MqsResponse::status(Status::NotFound)
                },
                Err(err) => {
                    error!("Failed to update queue {}, {:?}: {}", queue_name, config, err);
                    MqsResponse::status(Status::InternalServerError)
                }
            }
        }
    }
}

pub fn delete_queue(conn: DbConn, queue_name: &str) -> MqsResponse {
    info!("Deleting queue {}", queue_name);
    let deleted = Queue::delete_by_name(&conn, queue_name);
    match deleted {
        Ok(Some(queue)) => {
            info!("Deleted queue {}", queue_name);
            MqsResponse::json(&QueueConfigOutput::new(queue))
        },
        Ok(None) => {
            info!("Message {} was not found", queue_name);
            MqsResponse::status(Status::NotFound)
        },
        Err(err) => {
            error!("Failed to delete queue {}: {}", queue_name, err);
            MqsResponse::status(Status::InternalServerError)
        },
    }
}

#[derive(Debug)]
pub struct QueuesRange {
    offset: Option<i64>,
    limit: Option<i64>,
}

impl QueuesRange {
    pub fn from_hyper(req: hyper::Request<Body>) -> Result<QueuesRange, String> {
        let query = req.uri().query().unwrap_or("");
        let mut query_params = HashMap::new();
        for param in query.split("&").into_iter() {
            if param.is_empty() {
                continue;
            }

            // TODO: add proper query param parsing and url param decoding
            let mut i = param.splitn(2, "=").into_iter();
            let name = i.next().unwrap_or("");
            let value = i.next().unwrap_or("");
            debug_assert!(i.next().is_none());
            query_params.insert(name, value);
        }
        let offset = query_params.get("offset").map_or_else(|| Ok(None), |s| s.parse().map_or_else(|e| Err(e), |v| Ok(Some(v))));
        let limit = query_params.get("limit").map_or_else(|| Ok(None), |s| s.parse().map_or_else(|e| Err(e), |v| Ok(Some(v))));

        match (offset, limit) {
            (Err(err), _) => Err(format!("invalid value for number field offset: {}", err)),
            (_, Err(err)) => Err(format!("invalid value for number field limit: {}", err)),
            (Ok(offset), Ok(limit)) => Ok(QueuesRange {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct QueuesResponse {
    pub queues: Vec<QueueConfigOutput>,
    pub total: i64,
}

fn list_queues_and_count(conn: DbConn, range: &QueuesRange) -> QueryResult<QueuesResponse> {
    let queues = Queue::list(&conn, range.offset, range.limit)?;
    let total = Queue::count(&conn)?;
    Ok(QueuesResponse {
        queues: queues.into_iter().map(|queue| QueueConfigOutput::new(queue)).collect(),
        total,
    })
}

pub fn list_queues(conn: DbConn, range: Result<QueuesRange, String>) -> MqsResponse {
    match range {
        Err(err) => MqsResponse::error_owned(err),
        Ok(range) => match list_queues_and_count(conn, &range) {
            Ok(response) => MqsResponse::json(&response),
            Err(err) => {
                error!("Failed to read range of queues {:?}-{:?}: {}", range.offset, range.limit, err);

                MqsResponse::status(Status::InternalServerError)
            },
        },
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueStatus {
    pub messages: i64,
    pub visible_messages: i64,
    pub oldest_message_age: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueDescription {
    pub name: String,
    pub redrive_policy: Option<QueueRedrivePolicy>,
    pub retention_timeout: i64,
    pub visibility_timeout: i64,
    pub message_delay: i64,
    pub message_deduplication: bool,
    pub status: QueueStatus,
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

pub fn describe_queue(conn: DbConn, queue_name: &str) -> MqsResponse {
    match Queue::describe(&conn, queue_name) {
        Err(err) => {
            error!("Failed to describe queue {}: {}", queue_name, err);
            MqsResponse::status(Status::InternalServerError)
        },
        Ok(None) => MqsResponse::status(Status::NotFound),
        Ok(Some(description)) => MqsResponse::json(&QueueDescription::new(description.queue, description.messages, description.visible_messages, description.oldest_message_age)),
    }
}
