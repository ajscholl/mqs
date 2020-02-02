use rocket::http::{Header, Status};
use rocket::{response, Request, Response, Data};
use rocket::request::{FromRequest, Outcome};
use rocket::response::Responder;
use std::io::{Cursor, Read};
use hyper::HeaderMap;
use hyper::header::{HeaderName, HeaderValue};

use crate::multipart;
use crate::connection::DbConn;
use crate::models::message::{Message, NewMessage, MessageInput};
use crate::routes::{ErrorResponder, StatusResponder};
use crate::models::queue::Queue;
use std::borrow::Borrow;

const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;
pub const DEFAULT_CONTENT_TYPE: &'static str = "application/octet-stream";

#[derive(Debug)]
pub struct MessageContentType {
    content_type: String,
}

impl <'a, 'r> FromRequest<'a, 'r> for MessageContentType {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> Outcome<Self, <Self as FromRequest<'a, 'r>>::Error> {
        let content_type = request.headers().get_one("Content-Type");
        Outcome::Success(MessageContentType {
            content_type: content_type.unwrap_or(DEFAULT_CONTENT_TYPE).to_string(),
        })
    }
}

#[post("/messages/<queue_name>", data = "<message>")]
pub fn publish_messages(conn: DbConn, queue_name: String, message: Data, content_type: MessageContentType) -> StatusResponder {
    let mut message_content = String::new();
    match message.open().take(MAX_MESSAGE_SIZE).read_to_string(&mut message_content) {
        Err(err) => {
            error!("Failed to read message body into string: {}", err);
            return StatusResponder::new(Status::BadRequest);
        },
        Ok(_) => {},
    }
    let messages = if let Some(boundary) = multipart::is_multipart(&content_type.content_type) {
        multipart::parse(&boundary, &message_content)
    } else {
        Ok(vec![
            ({
                 let mut headers = HeaderMap::new();
                 if let Ok(value) = HeaderValue::from_str(&content_type.content_type) {
                     headers.insert(HeaderName::from_static("content-type"), value);
                 }
                 headers
             }, message_content.borrow()),
        ])
    };
    match messages {
        Err(err) => {
            error!("Failed to understand request body: {}", err);
            StatusResponder::new(Status::BadRequest)
        },
        Ok(messages) => match Queue::find_by_name(&conn, &queue_name) {
            Err(err) => {
                error!("Failed to find queue {} for new message: {}", &queue_name, err);
                StatusResponder::new(Status::InternalServerError)
            },
            Ok(None) => {
                error!("No queue with name {} found for new message", &queue_name);
                StatusResponder::new(Status::NotFound)
            },
            Ok(Some(queue)) => {
                let mut created_some = false;

                for message in messages {
                    info!("Inserting new message into queue {}", &queue_name);
                    match NewMessage::insert(&conn, &queue, &MessageInput {
                        content_type: message.0.get("Content-Type")
                            .map_or_else(|| DEFAULT_CONTENT_TYPE, |v| v.to_str().unwrap_or(DEFAULT_CONTENT_TYPE)),
                        payload: message.1,
                    }) {
                        Err(err) => {
                            error!("Failed to insert new message into queue {}: {}", &queue_name, err);
                            return StatusResponder::new(Status::InternalServerError);
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
                    StatusResponder::new(Status::Created)
                } else {
                    StatusResponder::new(Status::Ok)
                }
            },
        },
    }
}

#[derive(Debug)]
pub struct MessageResponder {
    messages: Vec<Message>,
}

impl MessageResponder {
    fn new(messages: Vec<Message>) -> MessageResponder {
        MessageResponder { messages }
    }
}

impl <'r> Responder<'r> for MessageResponder {
    fn respond_to(mut self, _req: &Request) -> response::Result<'r> {
        if self.messages.len() == 1 {
            let message = self.messages.pop().unwrap();

            return Response::build()
                .status(Status::Ok)
                .header(Header::new("Content-Type", message.content_type))
                .header(Header::new("X-MQS-MESSAGE-ID", message.id.to_string()))
                .sized_body(Cursor::new(message.payload))
                .ok();
        }

        let messages = self.messages.into_iter().map(|message| {
            let mut headers = HeaderMap::new();
            if let Ok(content_type) = HeaderValue::from_str(&message.content_type) {
                headers.insert(HeaderName::from_static("content-type"), content_type);
            }
            if let Ok(id) = HeaderValue::from_str(&message.id.to_string()) {
                headers.insert(HeaderName::from_static("x-mqs-message-id"), id);
            }
            (headers, message.payload.as_bytes().to_vec())
        }).collect();
        let (boundary, body) = multipart::encode(&messages);

        Response::build()
            .status(Status::Ok)
            .header(Header::new("Content-Type", format!("multipart/mixed; boundary={}", &boundary)))
            .sized_body(Cursor::new(body))
            .ok()
    }
}

pub struct MessageCount(i64);

impl <'a, 'r> FromRequest<'a, 'r> for MessageCount {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> Outcome<Self, <Self as FromRequest<'a, 'r>>::Error> {
        if let Some(max_messages) = request.headers().get_one("X-MQS-MAX-MESSAGES") {
            match max_messages.parse() {
                Err(_) => Outcome::Failure((Status::BadRequest, ())),
                Ok(n) => if n > 0 && n < 1000 {
                    Outcome::Success(MessageCount(n))
                } else {
                    Outcome::Failure((Status::BadRequest, ()))
                },
            }
        } else {
            Outcome::Success(MessageCount(1))
        }
    }
}


#[get("/messages/<queue_name>")]
pub fn receive_messages(conn: DbConn, queue_name: String, message_count: Result<MessageCount, ()>) -> Result<MessageResponder, Result<StatusResponder, ErrorResponder>> {
    match message_count {
        Err(_) => Err(Err(ErrorResponder::new("Failed to parse message count"))),
        Ok(count) => match Queue::find_by_name(&conn, &queue_name) {
            Err(err) => {
                error!("Failed to find queue {} for message receive: {}", &queue_name, err);
                Err(Ok(StatusResponder::new(Status::InternalServerError)))
            },
            Ok(None) => {
                error!("No queue with name {} found for message receive", &queue_name);
                Err(Ok(StatusResponder::new(Status::NotFound)))
            },
            Ok(Some(queue)) => {
                debug!("Reading 1 message from queue {}", &queue_name);
                match Message::get_from_queue(&conn, &queue, count.0) {
                    Ok(messages) => match messages.len() {
                        0 => Err(Ok(StatusResponder::new(Status::NoContent))),
                        _ => Ok(MessageResponder::new(messages)),
                    },
                    Err(err) => {
                        error!("Failed reading message from queue {}: {}", &queue_name, err);
                        Err(Ok(StatusResponder::new(Status::InternalServerError)))
                    },
                }
            },
        },
    }
}

#[delete("/messages/<message_id>")]
pub fn delete_message(conn: DbConn, message_id: String) -> Result<StatusResponder, ErrorResponder> {
    match uuid::Uuid::parse_str(&message_id) {
        Err(_) => Err(ErrorResponder::new("Message ID needs to be a UUID")),
        Ok(id) => Ok({
            info!("Deleting message {}", id);
            let deleted = Message::delete_by_id(&conn, id);
            match deleted {
                Ok(true) => {
                    info!("Deleted message {}", id);
                    StatusResponder::new(Status::Ok)
                },
                Ok(false) => {
                    info!("Message {} was not found", id);
                    StatusResponder::new(Status::NotFound)
                },
                Err(err) => {
                    error!("Failed to delete message {}: {}", id, err);
                    StatusResponder::new(Status::InternalServerError)
                },
            }
        }),
    }
}
