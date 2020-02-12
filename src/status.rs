use hyper::StatusCode;

#[derive(Debug, Clone, Copy)]
pub enum Status {
    Ok = 200,
    Created = 201,
    NoContent = 204,
    BadRequest = 400,
    NotFound = 404,
    Conflict = 409,
    InternalServerError = 500,
    ServiceUnavailable = 503,
}

impl Status {
    pub fn to_hyper(&self) -> StatusCode {
        StatusCode::from_u16(*self as u16).unwrap()
    }
}