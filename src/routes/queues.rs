use rocket::request::{FromRequest, Outcome};
use rocket::Request;
use rocket::http::Status;

use crate::connection::DbConn;
use crate::models::queue::{NewQueue, QueueInput};
use crate::routes::{ErrorResponder, StatusResponder};

#[derive(Debug)]
pub struct CreateQueueParams {
    max_receives: Option<i32>,
    dead_letter_queue: Option<String>,
    retention_timeout: i64,
    visibility_timeout: i64,
    message_delay: i64,
    content_based_deduplication: bool,
}

pub enum CreateQueueGuard {
    Params(CreateQueueParams),
    Error(&'static str),
}

fn parse_create_queue_request<'a, 'r>(request: &'a Request<'r>) -> Result<CreateQueueParams, &'static str> {
    let headers = request.headers();
    let max_receives = headers.get_one("X-MQS-MAX-RECEIVES")
        .map_or(Ok(None), |s| match s.parse() {
            Ok(n) => if n > 0 { Ok(Some(n)) } else { Err("X-MQS-MAX-RECEIVES must not be less than 0") },
            Err(_) => Err("X-MQS-MAX-RECEIVES must be a valid integer if set"),
        })?;
    let dead_letter_queue = headers.get_one("X-MQS-DEAD-LETTER-QUEUE")
        .map(|s| s.to_string());
    let retention_timeout = headers.get_one("X-MQS-RETENTION-SECONDS")
        .map_or(Err("X-MQS-RETENTION-SECONDS is required"), |s| match s.parse() {
            Ok(n) => if n > 0 { Ok(n) } else { Err("X-MQS-RETENTION-SECONDS must not be less than 0") },
            Err(_) => Err("X-MQS-RETENTION-SECONDS must be a valid integer"),
        })?;
    let visibility_timeout = headers.get_one("X-MQS-VISIBILITY-TIMEOUT-SECONDS")
        .map_or(Err("X-MQS-VISIBILITY-TIMEOUT-SECONDS is required"), |s| match s.parse() {
            Ok(n) => if n > 0 { Ok(n) } else { Err("X-MQS-VISIBILITY-TIMEOUT-SECONDS must not be less than 0") },
            Err(_) => Err("X-MQS-VISIBILITY-TIMEOUT-SECONDS must be a valid integer"),
        })?;
    let message_delay = headers.get_one("X-MQS-DELAY-SECONDS")
        .map_or(Err("X-MQS-DELAY-SECONDS is required"), |s| match s.parse() {
            Ok(n) => if n >= 0 { Ok(n) } else { Err("X-MQS-DELAY-SECONDS must not be less than 0") },
            Err(_) => Err("X-MQS-DELAY-SECONDS must be a valid integer"),
        })?;
    let content_based_deduplication = headers.get_one("X-MQS-CONTENT-BASED-DEDUPLICATION")
        .map_or(Err("X-MQS-CONTENT-BASED-DEDUPLICATION is required"), |s| match s {
            "false" => Ok(false),
            "true" => Ok(true),
            _ => Err("X-MQS-CONTENT-BASED-DEDUPLICATION needs to be true or false"),
        })?;

    if max_receives.is_some() != dead_letter_queue.is_some() {
        return Err(if max_receives.is_some() {
            "X-MQS-DEAD-LETTER-QUEUE is required if X-MQS-MAX-RECEIVES is set"
        } else {
            "X-MQS-MAX-RECEIVES is required if X-MQS-DEAD-LETTER-QUEUE is set"
        });
    }

    Ok(CreateQueueParams {
        max_receives,
        dead_letter_queue,
        retention_timeout,
        visibility_timeout,
        message_delay,
        content_based_deduplication,
    })
}

impl <'a, 'r> FromRequest<'a, 'r> for CreateQueueGuard {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> Outcome<Self, <Self as FromRequest<'a, 'r>>::Error> {
        Outcome::Success(match parse_create_queue_request(request) {
            Err(err) => CreateQueueGuard::Error(err),
            Ok(res) => CreateQueueGuard::Params(res),
        })
    }
}

#[put("/queues/<queue_name>")]
pub fn new_queue(conn: DbConn, queue_name: String, params: CreateQueueGuard) -> Result<StatusResponder, ErrorResponder> {
    match params {
        CreateQueueGuard::Error(err) => Err(ErrorResponder::new(err)),
        CreateQueueGuard::Params(queue_params) => {
            info!("Creating new queue {}", &queue_name);
            let created = NewQueue::insert(&conn, &QueueInput {
                name: &queue_name,
                max_receives: queue_params.max_receives,
                dead_letter_queue: &queue_params.dead_letter_queue,
                retention_timeout: queue_params.retention_timeout,
                visibility_timeout: queue_params.visibility_timeout,
                message_delay: queue_params.message_delay,
                content_based_deduplication: queue_params.content_based_deduplication,
            });

            Ok(match created {
                Ok(true) => {
                    info!("Created new queue {}", &queue_name);
                    StatusResponder::new(Status::Created)
                },
                Ok(false) => {
                    info!("Queue {} did already exist", &queue_name);
                    StatusResponder::new(Status::Conflict)
                },
                Err(err) => {
                    error!("Failed to create new queue {}, {:?}: {}", queue_name, queue_params, err);
                    StatusResponder::new(Status::InternalServerError)
                }
            })
        }
    }
}
