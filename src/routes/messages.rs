use rocket::http::{Header, Status};
use rocket::{response, Request, Response, Data};
use rocket::request::{FromRequest, Outcome};
use rocket::response::Responder;
use uuid::Uuid;
use std::io::{Cursor, Read};

use crate::connection::DbConn;
use crate::models::message::{Message, NewMessage, MessageInput};
use crate::routes::{ErrorResponder, StatusResponder};
use crate::models::queue::Queue;

const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;

#[derive(Debug)]
pub struct MessageContentType {
    content_type: String,
}

impl <'a, 'r> FromRequest<'a, 'r> for MessageContentType {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> Outcome<Self, <Self as FromRequest<'a, 'r>>::Error> {
        let content_type = request.headers().get_one("Content-Type");
        Outcome::Success(MessageContentType {
            content_type: content_type.unwrap_or("application/octet-stream").to_string(),
        })
    }
}

#[post("/messages/<queue_name>", data = "<message>")]
pub fn publish_message(conn: DbConn, queue_name: String, message: Data, content_type: MessageContentType) -> StatusResponder {
    match Queue::find_by_name(&conn, &queue_name) {
        Err(err) => {
            error!("Failed to find queue {} for new message: {}", &queue_name, err);
            StatusResponder::new(Status::InternalServerError)
        },
        Ok(None) => {
            error!("No queue with name {} found for new message", &queue_name);
            StatusResponder::new(Status::NotFound)
        },
        Ok(Some(queue)) => {
            let mut message_content = String::new();
            match message.open().take(MAX_MESSAGE_SIZE).read_to_string(&mut message_content) {
                Err(err) => {
                    error!("Failed to read message body into string: {}", err);
                    StatusResponder::new(Status::InternalServerError)
                },
                Ok(_) => {
                    info!("Inserting new message into queue {}", &queue_name);
                    match NewMessage::insert(&conn, &queue, &MessageInput {
                        content_type: &content_type.content_type,
                        payload: &message_content,
                    }) {
                        Err(err) => {
                            error!("Failed to insert new message into queue {}: {}", &queue_name, err);
                            StatusResponder::new(Status::InternalServerError)
                        },
                        Ok(true) => {
                            debug!("Published new message into queue {}", &queue_name);
                            StatusResponder::new(Status::Created)
                        },
                        Ok(false) => {
                            debug!("New message already exists in queue {}", &queue_name);
                            StatusResponder::new(Status::Ok)
                        },
                    }
                },
            }
        },
    }
}

#[derive(Debug)]
pub struct MessageResponder {
    id: Uuid,
    content_type: String,
    payload: String,
}

impl MessageResponder {
    fn new(message: Message) -> MessageResponder {
        MessageResponder {
            id: message.id,
            content_type: message.content_type,
            payload: message.payload,
        }
    }
}

impl <'r> Responder<'r> for MessageResponder {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        Response::build()
            .status(Status::Ok)
            .header(Header::new("Content-Type", self.content_type))
            .header(Header::new("X-MQS-MESSAGE-ID", self.id.to_string()))
            .sized_body(Cursor::new(self.payload))
            .ok()
    }
}

#[get("/messages/<queue_name>")]
pub fn receive_message(conn: DbConn, queue_name: String) -> Result<MessageResponder, StatusResponder> {
    match Queue::find_by_name(&conn, &queue_name) {
        Err(err) => {
            error!("Failed to find queue {} for message receive: {}", &queue_name, err);
            Err(StatusResponder::new(Status::InternalServerError))
        },
        Ok(None) => {
            error!("No queue with name {} found for message receive", &queue_name);
            Err(StatusResponder::new(Status::NotFound))
        },
        Ok(Some(queue)) => {
            debug!("Reading 1 message from queue {}", &queue_name);
            match Message::get_from_queue(&conn, &queue, 1) {
                Ok(mut messages) => match messages.pop() {
                    None => Err(StatusResponder::new(Status::NoContent)),
                    Some(message) => Ok(MessageResponder::new(message)),
                },
                Err(err) => {
                    error!("Failed reading message from queue {}: {}", &queue_name, err);
                    Err(StatusResponder::new(Status::InternalServerError))
                },
            }
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
