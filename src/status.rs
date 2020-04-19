use hyper::StatusCode;

#[derive(Debug, Clone, Copy)]
pub enum Status {
    Ok                  = 200,
    Created             = 201,
    NoContent           = 204,
    BadRequest          = 400,
    NotFound            = 404,
    Conflict            = 409,
    PayloadTooLarge     = 413,
    InternalServerError = 500,
    ServiceUnavailable  = 503,
}

impl Status {
    pub fn to_hyper(&self) -> StatusCode {
        StatusCode::from_u16(*self as u16).unwrap()
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
            assert_eq!(status.to_hyper().as_u16(), *status as u16);
        }
    }
}
