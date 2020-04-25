use hyper::{
    header::{HeaderValue, CONNECTION, CONTENT_TYPE, SERVER},
    Body,
    Request,
    Response,
};
use std::convert::Infallible;

use crate::{read_body, router::Router, status::Status};

pub async fn handle<T, S>(
    conn: Option<T>,
    source: S,
    router: &Router<(T, S)>,
    max_message_size: usize,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let mut response = if let Some(conn) = conn {
        let segments = req.uri().path().split("/").into_iter();
        {
            if let Some(handler) = router.route(req.method(), segments) {
                let body = if handler.needs_body() {
                    read_body(req.body_mut(), Some(max_message_size)).await
                } else {
                    Ok(Some(Vec::new()))
                };
                match body {
                    Err(err) => {
                        error!("Failed to read message body: {}", err);

                        let mut response = Response::new(Body::from("{\"error\":\"Internal server error\"}"));
                        response
                            .headers_mut()
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        *response.status_mut() = Status::InternalServerError.to_hyper();
                        response
                    },
                    Ok(None) => {
                        warn!("Body was larger than max allowed size ({})", max_message_size);

                        let mut response = Response::new(Body::from("{\"error\":\"Payload too large\"}"));
                        response
                            .headers_mut()
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        *response.status_mut() = Status::PayloadTooLarge.to_hyper();
                        response
                    },
                    Ok(Some(body)) => {
                        info!("Found handler for request {} {}", req.method(), req.uri().path());

                        handler.handle((conn, source), req, body).await
                    },
                }
            } else {
                error!("No handler found for request {} {}", req.method(), req.uri().path());

                let mut response = Response::new(Body::from("{\"error\":\"No handler found for request\"}"));
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                *response.status_mut() = Status::NotFound.to_hyper();
                response
            }
        }
    } else {
        error!(
            "No database connection available for request {} {}",
            req.method(),
            req.uri().path()
        );

        let mut response = Response::new(Body::from("{\"error\":\"Service unavailable, try again later\"}"));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        *response.status_mut() = Status::ServiceUnavailable.to_hyper();
        response
    };
    response.headers_mut().insert(SERVER, HeaderValue::from_static("mqs"));
    // TODO: should we add this header every time or only when seeing it from the client / on HTTP/1.1?
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}
