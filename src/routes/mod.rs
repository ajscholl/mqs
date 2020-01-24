use rocket::{response, Response};
use rocket::response::Responder;
use rocket::Request;
use rocket::http::{Header, Status};
use std::io::Cursor;
use serde::Serialize;

pub mod health;
pub mod messages;
pub mod queues;

#[derive(Serialize, Debug)]
pub struct ErrorResponse<'a> {
    error: &'a str,
}

#[derive(Debug)]
pub enum ErrorResponder {
    StaticError(&'static str),
    OwnedError(String),
}

impl ErrorResponder {
    pub fn new(error: &'static str) -> ErrorResponder {
        ErrorResponder::StaticError(error)
    }

    pub fn new_owned(error: String) -> ErrorResponder {
        ErrorResponder::OwnedError(error)
    }
}

impl <'r> Responder<'r> for ErrorResponder {
    fn respond_to(self, req: &Request) -> response::Result<'r> {
        match self {
            ErrorResponder::StaticError(error) => JsonResponder::new(Status::BadRequest, ErrorResponse { error }).respond_to(req),
            ErrorResponder::OwnedError(error) => JsonResponder::new(Status::BadRequest, ErrorResponse { error: &error }).respond_to(req),
        }
    }
}

#[derive(Debug)]
pub struct StatusResponder {
    status: Status,
}

impl StatusResponder {
    pub fn new(status: Status) -> StatusResponder {
        StatusResponder {
            status,
        }
    }
}

impl <'r> Responder<'r> for StatusResponder {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        Response::build().status(self.status).ok()
    }
}

#[derive(Debug)]
pub struct JsonResponder<T> {
    status: Status,
    body: T,
}

impl <T> JsonResponder<T> {
    pub fn new(status: Status, body: T) -> JsonResponder<T> {
        JsonResponder {
            status,
            body,
        }
    }
}

impl <'r, T> Responder<'r> for JsonResponder<T> where T: Serialize {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        match serde_json::to_string(&self.body) {
            Err(err) => {
                error!("Failed to serialize json response: {}", err);

                Response::build().status(Status::InternalServerError).ok()
            },
            Ok(body) => Response::build()
                .status(self.status)
                .header(Header::new("Content-Type", "application/json"))
                .sized_body(Cursor::new(body))
                .ok()
        }
    }
}
