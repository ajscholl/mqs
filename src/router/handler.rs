use hyper::{Request, Body, Response, Method, StatusCode};
use std::convert::Infallible;
use crate::router::{Router, WildcardRouter};
use hyper::header::{HeaderValue, SERVER, CONNECTION, CONTENT_TYPE};
use crate::router::queues::{DescribeQueueHandler, CreateQueueHandler, UpdateQueueHandler, DeleteQueueHandler, ListQueuesHandler};
use crate::router::messages::{ReceiveMessagesHandler, PublishMessagesHandler, DeleteMessageHandler};
use crate::router::health::HealthHandler;
use crate::connection::DbConn;
use crate::client::Service;

struct QueuesSubRouter;

impl WildcardRouter<DbConn> for QueuesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<DbConn> {
        Router::new()
            .with_handler(Method::GET, DescribeQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::PUT, CreateQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::POST, UpdateQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::DELETE, DeleteQueueHandler { queue_name: segment.to_string() })
    }
}

struct MessagesSubRouter;

impl WildcardRouter<DbConn> for MessagesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<DbConn> {
        Router::new()
            .with_handler(Method::GET, ReceiveMessagesHandler { queue_name: segment.to_string() })
            .with_handler(Method::POST, PublishMessagesHandler { queue_name: segment.to_string() })
            .with_handler(Method::DELETE, DeleteMessageHandler { message_id: segment.to_string() })
    }
}

pub fn make_router() -> Router<DbConn> {
    Router::new()
        .with_route_simple("health", Method::GET, HealthHandler)
        .with_route("queues", Router::new_simple(Method::GET, ListQueuesHandler).with_wildcard(QueuesSubRouter))
        .with_route("messages", Router::new().with_wildcard(MessagesSubRouter))
}

pub async fn handle<T>(conn: Option<T>, router: &Router<T>, mut req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut response = if let Some(conn) = conn {
        let segments = req.uri().path().split("/").into_iter();
        {
            if let Some(handler) = router.route(req.method(), segments) {
                let body = if handler.needs_body() {
                    Service::read_body(req.body_mut()).await
                } else {
                    Ok(Vec::new())
                };
                match body {
                    Err(err) => {
                        error!("Failed to read message body: {}", err);

                        let mut response = Response::new(Body::from("{\"error\":\"Internal server error\"}"));
                        response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        *response.status_mut() = StatusCode::from_u16(500).unwrap();
                        response
                    },
                    Ok(body) => {
                        info!("Found handler for request {} {}", req.method(), req.uri().path());

                        handler.handle(conn, req, body)
                    },
                }
            } else {
                error!("No handler found for request {} {}", req.method(), req.uri().path());

                let mut response = Response::new(Body::from("{\"error\":\"No handler found for request\"}"));
                response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                *response.status_mut() = StatusCode::from_u16(404).unwrap();
                response
            }
        }
    } else {
        error!("No database connection available for request {} {}", req.method(), req.uri().path());

        let mut response = Response::new(Body::from("{\"error\":\"Service unavailable, try again later\"}"));
        response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        *response.status_mut() = StatusCode::from_u16(503).unwrap();
        response
    };
    response.headers_mut().insert(SERVER, HeaderValue::from_static("mqs"));
    // TODO: should we add this header every time or only when seeing it from the client / on HTTP/1.1?
    response.headers_mut().insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn health_router() {
        let router = make_router();
        let handler = router.route(&Method::GET, vec!["health"].into_iter());
        assert!(handler.is_some());
    }
}
