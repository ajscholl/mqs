use hyper::{
    header::{HeaderValue, CONTENT_ENCODING, CONTENT_TYPE},
    HeaderMap,
};

use crate::{
    models::{
        message::{MessageInput, MessageRepository},
        queue::QueueRepository,
    },
    multipart,
    routes::MqsResponse,
    status::Status,
    wait::MESSAGE_WAIT_QUEUE,
};

const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;
pub const DEFAULT_CONTENT_TYPE: &'static str = "application/octet-stream";

fn boundary_from_headers(headers: &HeaderMap<HeaderValue>) -> Option<String> {
    let content_type_header = headers.get(CONTENT_TYPE)?;
    let content_type = content_type_header.to_str().map_or_else(|_| None, |s| Some(s))?;
    multipart::is_multipart(content_type)
}

pub async fn publish_messages<R: QueueRepository + MessageRepository>(
    repo: R,
    queue_name: &String,
    message_content: &[u8],
    headers: HeaderMap<HeaderValue>,
) -> MqsResponse {
    let messages = if let Some(boundary) = boundary_from_headers(&headers) {
        multipart::parse(boundary.as_bytes(), message_content)
    } else {
        Ok(vec![(headers, message_content)])
    };
    let messages = match messages {
        Err(err) => {
            error!("Failed to understand request body: {}", err);
            return MqsResponse::status(Status::BadRequest);
        },
        Ok(messages) => messages,
    };
    let queue = match repo.find_by_name_cached(&queue_name) {
        Err(err) => {
            error!("Failed to find queue {} for new message: {}", &queue_name, err);
            return MqsResponse::status(Status::InternalServerError);
        },
        Ok(None) => {
            error!("No queue with name {} found for new message", &queue_name);
            return MqsResponse::status(Status::NotFound);
        },
        Ok(Some(queue)) => queue,
    };

    let mut created_some = false;

    for (message_headers, message_payload) in messages {
        info!("Inserting new message into queue {}", &queue_name);
        match repo.insert_message(&queue, &MessageInput {
            content_type:     message_headers
                .get(CONTENT_TYPE)
                .map_or_else(|| DEFAULT_CONTENT_TYPE, |v| v.to_str().unwrap_or(DEFAULT_CONTENT_TYPE)),
            content_encoding: message_headers
                .get(CONTENT_ENCODING)
                .map_or_else(|| None, |v| v.to_str().map_or_else(|_| None, |s| Some(s))),
            payload:          message_payload,
        }) {
            Err(err) => {
                error!("Failed to insert new message into queue {}: {}", &queue_name, err);
                return MqsResponse::status(Status::InternalServerError);
            },
            Ok(true) => {
                debug!("Published new message into queue {}", &queue_name);
                created_some = true;
            },
            Ok(false) => {
                debug!("New message already exists in queue {}", &queue_name);
            },
        }
    }

    if created_some {
        MESSAGE_WAIT_QUEUE.signal(&queue).await;
        MqsResponse::status(Status::Created)
    } else {
        MqsResponse::status(Status::Ok)
    }
}

pub struct MessageCount(pub i64);
pub struct MaxWaitTime(pub u64);

pub trait Source<R>: Send {
    fn get(&self) -> Option<R>;
}

pub async fn receive_messages<R: QueueRepository + MessageRepository, S: Source<R>>(
    repo: R,
    repo_source: S,
    queue_name: &str,
    message_count: Result<MessageCount, ()>,
    max_wait_time: Result<Option<MaxWaitTime>, ()>,
) -> MqsResponse {
    let count = match message_count {
        Err(_) => {
            return MqsResponse::error_static("Failed to parse message count");
        },
        Ok(count) => count,
    };
    let wait_time = match max_wait_time {
        Err(_) => {
            return MqsResponse::error_static("Failed to parse maximal wait time");
        },
        Ok(wait_time) => wait_time,
    };
    let queue = match repo.find_by_name_cached(queue_name) {
        Err(err) => {
            error!("Failed to find queue {} for message receive: {}", queue_name, err);
            return MqsResponse::status(Status::InternalServerError);
        },
        Ok(None) => {
            error!("No queue with name {} found for message receive", queue_name);
            return MqsResponse::status(Status::NotFound);
        },
        Ok(Some(queue)) => queue,
    };
    debug!("Reading {} message(s) from queue {}", count.0, queue_name);
    let mut messages = match repo.get_message_from_queue(&queue, count.0) {
        Ok(messages) => messages,
        Err(err) => {
            error!("Failed reading message from queue {}: {}", queue_name, err);
            return MqsResponse::status(Status::InternalServerError);
        },
    };
    std::mem::drop(repo);
    if let Some(wait_time) = wait_time {
        if messages.is_empty() && MESSAGE_WAIT_QUEUE.wait(&queue, wait_time.0).await {
            match repo_source.get() {
                None => {
                    warn!("Failed to get second database connection");
                },
                Some(repo) => match repo.get_message_from_queue(&queue, count.0) {
                    Ok(new_messages) => {
                        messages = new_messages;
                    },
                    Err(err) => {
                        error!("Failed reading message from queue {}: {}", queue_name, err);
                        return MqsResponse::status(Status::InternalServerError);
                    },
                },
            }
        }
    }
    if messages.is_empty() {
        MqsResponse::status(Status::NoContent)
    } else {
        MqsResponse::messages(messages)
    }
}

pub fn delete_message<R: MessageRepository>(repo: R, message_id: &str) -> MqsResponse {
    match uuid::Uuid::parse_str(message_id) {
        Err(_) => MqsResponse::error_static("Message ID needs to be a UUID"),
        Ok(id) => {
            info!("Deleting message {}", id);
            let deleted = repo.delete_message_by_id(id);
            match deleted {
                Ok(true) => {
                    info!("Deleted message {}", id);
                    MqsResponse::status(Status::Ok)
                },
                Ok(false) => {
                    info!("Message {} was not found", id);
                    MqsResponse::status(Status::NotFound)
                },
                Err(err) => {
                    error!("Failed to delete message {}: {}", id, err);
                    MqsResponse::status(Status::InternalServerError)
                },
            }
        },
    }
}
