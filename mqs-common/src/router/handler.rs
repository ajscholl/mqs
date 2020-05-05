use http::version::Version;
use hyper::{
    header::{HeaderValue, CONNECTION, CONTENT_TYPE, SERVER},
    Body,
    Request,
    Response,
};
use std::convert::Infallible;

use crate::{read_body, router::Router, Status};

/// Handle a single request using the given router.
///
/// If the given connection is `None`, an error response is returned.
/// If more than `max_message_size` bytes are send by the client, an
/// error response is returned.
///
/// ```
/// use async_trait::async_trait;
/// use hyper::{Body, Method, Request, Response};
/// use mqs_common::{
///     read_body,
///     router::{handle, Handler, Router},
///     test::make_runtime,
/// };
///
/// struct IntSource {
///     int: i32,
/// }
///
/// struct ExampleHandler {}
///
/// #[async_trait]
/// impl Handler<(i32, IntSource)> for ExampleHandler {
///     async fn handle(&self, args: (i32, IntSource), req: Request<Body>, body: Vec<u8>) -> Response<Body> {
///         Response::new(Body::from(format!("{} -> {}", args.0, args.1.int)))
///     }
/// }
///
/// fn main() {
///     make_runtime().block_on(async {
///         let router = Router::new_simple(Method::GET, ExampleHandler {});
///         let mut response = handle(None, IntSource { int: 5 }, &router, 100, Request::new(Body::default()))
///             .await
///             .unwrap();
///         assert_eq!(response.status(), 503);
///         assert_eq!(
///             read_body(response.body_mut(), None).await.unwrap().unwrap(),
///             "{\"error\":\"Service unavailable, try again later\"}".as_bytes()
///         );
///         let mut response = handle(
///             Some(42),
///             IntSource { int: 5 },
///             &router,
///             100,
///             Request::new(Body::default()),
///         )
///         .await
///         .unwrap();
///         assert_eq!(response.status(), 200);
///         assert_eq!(
///             read_body(response.body_mut(), None).await.unwrap().unwrap(),
///             "42 -> 5".as_bytes()
///         );
///     });
/// }
/// ```
pub async fn handle<T, S>(
    conn: Option<T>,
    source: S,
    router: &Router<(T, S)>,
    max_message_size: usize,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    let version = req.version();
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
                        *response.status_mut() = Status::InternalServerError.into();
                        response
                    },
                    Ok(None) => {
                        warn!("Body was larger than max allowed size ({})", max_message_size);

                        let mut response = Response::new(Body::from("{\"error\":\"Payload too large\"}"));
                        response
                            .headers_mut()
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        *response.status_mut() = Status::PayloadTooLarge.into();
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
                *response.status_mut() = Status::NotFound.into();
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
        *response.status_mut() = Status::ServiceUnavailable.into();
        response
    };
    response.headers_mut().insert(SERVER, HeaderValue::from_static("mqs"));
    if version <= Version::HTTP_11 {
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    }
    Ok(response)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        read_body,
        router::{Handler, Router},
        test::make_runtime,
    };
    use async_trait::async_trait;
    use hyper::{Body, Method, Request, Response};

    struct EchoHandler {}

    #[async_trait]
    impl Handler<(i32, ())> for EchoHandler {
        fn needs_body(&self) -> bool {
            true
        }

        async fn handle(&self, args: (i32, ()), _: Request<Body>, body: Vec<u8>) -> Response<Body> {
            Response::new(Body::from(format!(
                "{} -> {}",
                args.0,
                String::from_utf8(body).unwrap()
            )))
        }
    }

    #[test]
    fn test_handler() {
        make_runtime().block_on(async {
            let router = Router::new_simple(Method::GET, EchoHandler {});
            let mut response = handle(None, (), &router, 100, Request::new(Body::default()))
                .await
                .unwrap();
            assert_eq!(response.status(), 503);
            assert_eq!(
                read_body(response.body_mut(), None).await.unwrap().unwrap(),
                "{\"error\":\"Service unavailable, try again later\"}".as_bytes()
            );
            let mut response = handle(Some(42), (), &router, 100, Request::new(Body::default()))
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                read_body(response.body_mut(), None).await.unwrap().unwrap(),
                "42 -> ".as_bytes()
            );
            let mut response = handle(Some(42), (), &router, 3, Request::new(Body::from("hello".to_string())))
                .await
                .unwrap();
            assert_eq!(response.status(), 413);
            assert_eq!(
                read_body(response.body_mut(), None).await.unwrap().unwrap(),
                "{\"error\":\"Payload too large\"}".as_bytes()
            );
            let mut response = handle(
                Some(42),
                (),
                &Router::new(),
                3,
                Request::new(Body::from("hello".to_string())),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), 404);
            assert_eq!(
                read_body(response.body_mut(), None).await.unwrap().unwrap(),
                "{\"error\":\"No handler found for request\"}".as_bytes()
            );
        });
    }
}
