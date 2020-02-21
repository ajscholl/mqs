use hyper::HeaderMap;
use hyper::header::{HeaderValue, CONTENT_TYPE, CONTENT_ENCODING};

use crate::multipart;
use crate::connection::DbConn;
use crate::models::message::{Message, NewMessage, MessageInput};
use crate::routes::MqsResponse;
use crate::models::queue::Queue;
use crate::status::Status;

const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;
pub const DEFAULT_CONTENT_TYPE: &'static str = "application/octet-stream";

fn boundary_from_headers(headers: &HeaderMap<HeaderValue>) -> Option<String> {
    let content_type_header = headers.get(CONTENT_TYPE)?;
    let content_type = content_type_header.to_str().map_or_else(|_| None, |s| Some(s))?;
    multipart::is_multipart(content_type)
}

pub fn publish_messages(conn: DbConn, queue_name: &String, message_content: &[u8], headers: HeaderMap<HeaderValue>) -> MqsResponse {
    let messages = if let Some(boundary) = boundary_from_headers(&headers) {
        multipart::parse(boundary.as_bytes(), message_content)
    } else {
        Ok(vec![(headers, message_content)])
    };
    match messages {
        Err(err) => {
            error!("Failed to understand request body: {}", err);
            MqsResponse::status(Status::BadRequest)
        },
        Ok(messages) => match Queue::find_by_name_cached(&conn, &queue_name) {
            Err(err) => {
                error!("Failed to find queue {} for new message: {}", &queue_name, err);
                MqsResponse::status(Status::InternalServerError)
            },
            Ok(None) => {
                error!("No queue with name {} found for new message", &queue_name);
                MqsResponse::status(Status::NotFound)
            },
            Ok(Some(queue)) => {
                let mut created_some = false;

                for (message_headers, message_payload) in messages {
                    info!("Inserting new message into queue {}", &queue_name);
                    match NewMessage::insert(&conn, &queue, &MessageInput {
                        content_type: message_headers.get(CONTENT_TYPE)
                            .map_or_else(|| DEFAULT_CONTENT_TYPE, |v| v.to_str().unwrap_or(DEFAULT_CONTENT_TYPE)),
                        content_encoding: message_headers.get(CONTENT_ENCODING)
                            .map_or_else(|| None, |v| v.to_str().map_or_else(|_| None, |s| Some(s))),
                        payload: message_payload,
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
                    MqsResponse::status(Status::Created)
                } else {
                    MqsResponse::status(Status::Ok)
                }
            },
        },
    }
}

pub struct MessageCount(pub i64);

pub fn receive_messages(conn: DbConn, queue_name: &str, message_count: Result<MessageCount, ()>) -> MqsResponse {
    match message_count {
        Err(_) => MqsResponse::error_static("Failed to parse message count"),
        Ok(count) => match Queue::find_by_name_cached(&conn, queue_name) {
            Err(err) => {
                error!("Failed to find queue {} for message receive: {}", queue_name, err);
                MqsResponse::status(Status::InternalServerError)
            },
            Ok(None) => {
                error!("No queue with name {} found for message receive", queue_name);
                MqsResponse::status(Status::NotFound)
            },
            Ok(Some(queue)) => {
                debug!("Reading 1 message from queue {}", queue_name);
                match Message::get_from_queue(&conn, &queue, count.0) {
                    Ok(messages) => match messages.len() {
                        0 => MqsResponse::status(Status::NoContent),
                        _ => MqsResponse::messages(messages),
                    },
                    Err(err) => {
                        error!("Failed reading message from queue {}: {}", queue_name, err);
                        MqsResponse::status(Status::InternalServerError)
                    },
                }
            },
        },
    }
}

pub fn delete_message(conn: DbConn, message_id: &str) -> MqsResponse {
    match uuid::Uuid::parse_str(message_id) {
        Err(_) => MqsResponse::error_static("Message ID needs to be a UUID"),
        Ok(id) => {
            info!("Deleting message {}", id);
            let deleted = Message::delete_by_id(&conn, id);
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
