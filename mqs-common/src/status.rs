use hyper::StatusCode;

/// All possible status codes used by the server.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// HTTP 200 OK
    Ok                  = 200,
    /// HTTP 201 Created
    Created             = 201,
    /// HTTP 204 No Content
    NoContent           = 204,
    /// HTTP 400 Bad Request
    BadRequest          = 400,
    /// HTTP 404 Not Found
    NotFound            = 404,
    /// HTTP 409 Conflict
    Conflict            = 409,
    /// HTTP 413 Payload Too Large
    PayloadTooLarge     = 413,
    /// HTTP 500 Internal Server Error
    InternalServerError = 500,
    /// HTTP 503 Service Unavailable
    ServiceUnavailable  = 503,
}

impl From<&Status> for StatusCode {
    fn from(status: &Status) -> Self {
        match status {
            Status::Ok => Self::OK,
            Status::Created => Self::CREATED,
            Status::NoContent => Self::NO_CONTENT,
            Status::BadRequest => Self::BAD_REQUEST,
            Status::NotFound => Self::NOT_FOUND,
            Status::Conflict => Self::CONFLICT,
            Status::PayloadTooLarge => Self::PAYLOAD_TOO_LARGE,
            Status::InternalServerError => Self::INTERNAL_SERVER_ERROR,
            Status::ServiceUnavailable => Self::SERVICE_UNAVAILABLE,
        }
    }
}

impl From<Status> for StatusCode {
    fn from(status: Status) -> Self {
        Self::from(&status)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hyper_identity() {
        let statuses = [
            Status::Ok,
            Status::Created,
            Status::NoContent,
            Status::BadRequest,
            Status::NotFound,
            Status::Conflict,
            Status::PayloadTooLarge,
            Status::InternalServerError,
            Status::ServiceUnavailable,
        ];

        for status in &statuses {
            assert_eq!(StatusCode::from(*status).as_u16(), *status as u16);
        }
    }
}
