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
    use diesel::QueryResult;
    use crate::models::queue::{QueueInput, QueueDescription, Queue, QueueSource};
    use uuid::Uuid;
    use crate::models::message::{Message, MessageInput};
    use diesel::result::Error;

    struct TestRepo {
        health: bool,
    }

    impl HealthCheckRepository for TestRepo {
        fn check_health(&self) -> bool {
            self.health
        }
    }

    impl MessageRepository for TestRepo {
        fn insert_message(&self, queue: &Queue, input: &MessageInput) -> QueryResult<bool> {
            unimplemented!()
        }

        fn get_message_from_queue(&self, queue: &Queue, count: i64) -> QueryResult<Vec<Message>> {
            unimplemented!()
        }

        fn move_message_to_queue(&self, ids: Vec<Uuid>, new_queue: &str) -> QueryResult<usize> {
            unimplemented!()
        }

        fn delete_message_by_id(&self, id: Uuid) -> QueryResult<bool> {
            unimplemented!()
        }

        fn delete_messages_by_ids(&self, ids: Vec<Uuid>) -> QueryResult<usize> {
            unimplemented!()
        }
    }

    impl QueueSource for TestRepo {
        fn find_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            unimplemented!()
        }
    }

    impl QueueRepository for TestRepo {
        fn insert_queue(&self, queue: &QueueInput) -> QueryResult<Option<Queue>> {
            unimplemented!()
        }

        fn count_queues(&self) -> QueryResult<i64> {
            unimplemented!()
        }

        fn describe_queue(&self, name: &str) -> QueryResult<Option<QueueDescription>> {
            unimplemented!()
        }

        fn list_queues(&self, offset: Option<i64>, limit: Option<i64>) -> QueryResult<Vec<Queue>> {
            unimplemented!()
        }

        fn update_queue(&self, queue: &QueueInput) -> QueryResult<Option<Queue>> {
            unimplemented!()
        }

        fn delete_queue_by_name(&self, name: &str) -> QueryResult<Option<Queue>> {
            unimplemented!()
        }
    }

    #[test]
    fn health_router() {
        let router = make_router::<TestRepo>();
        let handler = router.route(&Method::GET, vec!["health"].into_iter());
        assert!(handler.is_some());
    }
}
