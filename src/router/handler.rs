use hyper::{Request, Body, Response, Method, StatusCode};
use hyper::header::{HeaderValue, SERVER, CONNECTION, CONTENT_TYPE};
use std::convert::Infallible;

use crate::router::{Router, WildcardRouter};
use crate::router::queues::{DescribeQueueHandler, CreateQueueHandler, UpdateQueueHandler, DeleteQueueHandler, ListQueuesHandler};
use crate::router::messages::{ReceiveMessagesHandler, PublishMessagesHandler, DeleteMessageHandler};
use crate::router::health::HealthHandler;
use crate::client::Service;
use crate::models::queue::QueueRepository;
use crate::models::message::MessageRepository;
use crate::models::health::HealthCheckRepository;

struct QueuesSubRouter;

impl <R: QueueRepository> WildcardRouter<R> for QueuesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<R> {
        Router::new()
            .with_handler(Method::GET, DescribeQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::PUT, CreateQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::POST, UpdateQueueHandler { queue_name: segment.to_string() })
            .with_handler(Method::DELETE, DeleteQueueHandler { queue_name: segment.to_string() })
    }
}

struct MessagesSubRouter;

impl <R: QueueRepository + MessageRepository> WildcardRouter<R> for MessagesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<R> {
        Router::new()
            .with_handler(Method::GET, ReceiveMessagesHandler { queue_name: segment.to_string() })
            .with_handler(Method::POST, PublishMessagesHandler { queue_name: segment.to_string() })
            .with_handler(Method::DELETE, DeleteMessageHandler { message_id: segment.to_string() })
    }
}

pub fn make_router<R: QueueRepository + MessageRepository + HealthCheckRepository>() -> Router<R> {
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
    use crate::models::test::TestRepo;
    use crate::routes::test::read_body;
    use crate::status::Status;
    use std::sync::Arc;
    use crate::models::queue::QueueInput;
    use hyper::header::HeaderName;

    #[test]
    fn health_router() {
        let router = make_router::<TestRepo>();
        let handler = router.route(&Method::GET, vec!["health"].into_iter());
        assert!(handler.is_some());
        let handler = handler.unwrap();
        {
            let mut response = handler.handle(TestRepo::new(), Request::new(Body::default()), Vec::new());
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body, "green".as_bytes().to_vec());
        }
        {
            let mut repo = TestRepo::new();
            repo.set_health(false);
            let mut response = handler.handle(repo, Request::new(Body::default()), Vec::new());
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body, "red".as_bytes().to_vec());
        }
    }

    #[test]
    fn queues_router() {
        let repo = Arc::new(TestRepo::new());
        let router = make_router::<Arc<TestRepo>>();
        let create_handler = router.route(&Method::PUT, vec!["queues", "my-queue"].into_iter());
        assert!(create_handler.is_some());
        let create_handler = create_handler.unwrap();
        {
            let mut response = create_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                "{\"retention_timeout\": 600, \"visibility_timeout\": 30, \"message_delay\": 5, \"message_deduplication\": false}".as_bytes().to_vec(),
            );
            assert_eq!(Status::Created.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false}".as_bytes().to_vec(),
            );
        }
        {
            let mut response = create_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                "{\"retention_timeout\": 600, \"visibility_timeout\": 60, \"message_delay\": 5, \"message_deduplication\": false}".as_bytes().to_vec(),
            );
            assert_eq!(Status::Conflict.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body.len(),
                0,
            );
        }
        let get_handler = router.route(&Method::GET, vec!["queues", "my-queue"].into_iter());
        assert!(get_handler.is_some());
        let get_handler = get_handler.unwrap();
        {
            let mut response = get_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false,\"status\":{\"messages\":0,\"visible_messages\":0,\"oldest_message_age\":0}}".as_bytes().to_vec(),
            );
        }
        let list_handler = router.route(&Method::GET, vec!["queues"].into_iter());
        assert!(list_handler.is_some());
        let list_handler = list_handler.unwrap();
        {
            let mut response = list_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"queues\":[{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false}],\"total\":1}".as_bytes().to_vec(),
            );
        }
        let update_handler = router.route(&Method::POST, vec!["queues", "my-queue"].into_iter());
        assert!(update_handler.is_some());
        let update_handler = update_handler.unwrap();
        {
            let mut response = update_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                "{\"retention_timeout\": 30, \"visibility_timeout\": 10, \"message_delay\": 2, \"message_deduplication\": true}".as_bytes().to_vec(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":30,\"visibility_timeout\":10,\"message_delay\":2,\"message_deduplication\":true}".as_bytes().to_vec(),
            );
        }
        let delete_handler = router.route(&Method::DELETE, vec!["queues", "my-queue"].into_iter());
        assert!(delete_handler.is_some());
        let delete_handler = delete_handler.unwrap();
        {
            let mut response = delete_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":30,\"visibility_timeout\":10,\"message_delay\":2,\"message_deduplication\":true}".as_bytes().to_vec(),
            );
        }
        {
            let mut response = get_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::NotFound.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body.len(),
                0,
            );
        }
    }

    #[test]
    fn messages_router() {
        let repo = Arc::new(TestRepo::new());
        repo.insert_queue(&QueueInput {
            name: "my-queue",
            max_receives: None,
            dead_letter_queue: None,
            retention_timeout: 100,
            visibility_timeout: 10,
            message_delay: 0,
            content_based_deduplication: false,
        }).unwrap().unwrap();
        let router = make_router::<Arc<TestRepo>>();
        let publish_handler = router.route(&Method::POST, vec!["messages", "my-queue"].into_iter());
        assert!(publish_handler.is_some());
        let publish_handler = publish_handler.unwrap();
        {
            let mut response = publish_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                "{\"content\": \"my message\"}".as_bytes().to_vec(),
            );
            assert_eq!(Status::Created.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body.len(),
                0,
            );
        }
        let receive_handler = router.route(&Method::GET, vec!["messages", "my-queue"].into_iter());
        assert!(receive_handler.is_some());
        let receive_handler = receive_handler.unwrap();
        let message_id = {
            let mut response = receive_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                "{\"content\": \"my message\"}".as_bytes().to_vec(),
            );
            let response_message_id = response.headers().get(HeaderName::from_static("x-mqs-message-id"));
            assert!(response_message_id.is_some());
            response_message_id.unwrap().to_str().unwrap().to_string()
        };
        {
            let delete_handler = router.route(&Method::DELETE, vec!["messages", &message_id].into_iter());
            assert!(delete_handler.is_some());
            let delete_handler = delete_handler.unwrap();
            let mut response = delete_handler.handle(
                repo.clone(),
                Request::new(Body::default()),
                Vec::new(),
            );
            assert_eq!(Status::Ok.to_hyper(), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body.len(),
                0,
            );
        }
    }
}
