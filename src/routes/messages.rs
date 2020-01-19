use rocket::http::Status;
use rocket::Request;
use rocket::request::{FromRequest, Outcome};

use crate::connection::DbConn;
use crate::models::message::{Message, NewMessage, MessageInput};
use crate::routes::{ErrorResponder, StatusResponder};
use crate::models::queue::Queue;

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
pub fn publish_messages(conn: DbConn, queue_name: String, message: String, content_type: MessageContentType) -> StatusResponder {
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
            info!("Inserting new message into queue {}", &queue_name);
            match NewMessage::insert(&conn, &queue, &MessageInput {
                content_type: &content_type.content_type,
                payload: &message,
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
