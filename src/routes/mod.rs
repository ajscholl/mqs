use rocket_contrib::json::Json;
use rocket::{response, Response};
use rocket::response::Responder;
use rocket::Request;
use rocket::http::Status;

pub mod health;
pub mod messages;
pub mod queues;

#[derive(Serialize, Debug)]
pub struct ErrorResponse {
    error: &'static str,
}

#[derive(Responder, Debug)]
#[response(status = 400, content_type = "json")]
pub struct ErrorResponder {
    response: Json<ErrorResponse>,
}

impl ErrorResponder {
    pub fn new(error: &'static str) -> ErrorResponder {
        ErrorResponder {
            response: Json(ErrorResponse {
                error,
            }),
        }
    }
}

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

impl<'r> Responder<'r> for StatusResponder {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        Response::build().status(self.status).ok()
    }
}
