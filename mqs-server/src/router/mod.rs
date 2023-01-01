use hyper::Method;
use mqs_common::{
    connection::Source,
    router::{Router, WildcardRouter},
};

use crate::{
    models::{health::HealthCheckRepository, message::MessageRepository, queue::QueueRepository},
    router::{
        messages::{DeleteMessageHandler, PublishMessagesHandler, ReceiveMessagesHandler},
        queues::{CreateQueueHandler, DeleteQueueHandler, DescribeQueueHandler, ListQueuesHandler, UpdateQueueHandler},
    },
};

pub mod health;
mod messages;
mod queues;

struct QueuesSubRouter;

impl<R: QueueRepository, S: Source<R>> WildcardRouter<(R, S)> for QueuesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<(R, S)> {
        Router::default()
            .with_handler(Method::GET, DescribeQueueHandler {
                queue_name: segment.to_string(),
            })
            .with_handler(Method::PUT, CreateQueueHandler {
                queue_name: segment.to_string(),
            })
            .with_handler(Method::POST, UpdateQueueHandler {
                queue_name: segment.to_string(),
            })
            .with_handler(Method::DELETE, DeleteQueueHandler {
                queue_name: segment.to_string(),
            })
    }
}

struct MessagesSubRouter;

impl<R: QueueRepository + MessageRepository, S: Source<R>> WildcardRouter<(R, S)> for MessagesSubRouter {
    fn with_segment(&self, segment: &str) -> Router<(R, S)> {
        Router::default()
            .with_handler(Method::GET, ReceiveMessagesHandler {
                queue_name: segment.to_string(),
            })
            .with_handler(Method::POST, PublishMessagesHandler {
                queue_name: segment.to_string(),
            })
            .with_handler(Method::DELETE, DeleteMessageHandler {
                message_id: segment.to_string(),
            })
    }
}

/// Create a new instance of the router.
#[must_use]
pub fn make<R: QueueRepository + MessageRepository + HealthCheckRepository, S: Source<R>>() -> Router<(R, S)> {
    Router::default()
        .with_route_simple("health", Method::GET, health::Handler)
        .with_route(
            "queues",
            Router::new_simple(Method::GET, ListQueuesHandler).with_wildcard(QueuesSubRouter),
        )
        .with_route("messages", Router::default().with_wildcard(MessagesSubRouter))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        make_router,
        models::{
            queue::QueueInput,
            test::{TestRepo, TestRepoSource},
        },
    };
    use hyper::{Body, Request, Response, StatusCode};
    use mqs_common::{
        router::Handler,
        test::{make_runtime, read_body},
        MessageIdHeader,
        Status,
    };
    use std::sync::Arc;

    fn run_handler<'a>(
        handler: Arc<dyn Handler<(TestRepo, &'a TestRepoSource)>>,
        source: &'a TestRepoSource,
    ) -> Response<Body> {
        run_handler_with(handler, source, Vec::new())
    }

    fn run_handler_with<'a>(
        handler: Arc<dyn Handler<(TestRepo, &'a TestRepoSource)>>,
        source: &'a TestRepoSource,
        body: Vec<u8>,
    ) -> Response<Body> {
        let rt = make_runtime();
        rt.block_on(async {
            let repo = source
                .get()
                .expect("The test repo source should always return a repository");

            handler
                .handle((repo, source), Request::new(Body::default()), body)
                .await
        })
    }

    #[test]
    fn health_router() {
        let source = TestRepoSource::new();
        let router = make_router::<TestRepo, &TestRepoSource>();
        let handler = router.route(&Method::GET, vec!["health"].into_iter());
        assert!(handler.is_some());
        let handler = handler.expect("handler should have been found");
        {
            let mut response = run_handler(Arc::clone(&handler), &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.as_slice(), b"green");
        }
        {
            source.get().unwrap().set_health(false);
            let mut response = run_handler(handler, &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.as_slice(), b"red");
        }
    }

    #[test]
    fn queues_router() {
        let source = TestRepoSource::new();
        let router = make_router::<TestRepo, &TestRepoSource>();
        let create_handler = router.route(&Method::PUT, vec!["queues", "my-queue"].into_iter());
        assert!(create_handler.is_some());
        let create_handler = create_handler.unwrap();
        {
            let mut response = run_handler_with(
                create_handler.clone(),
                &source,
                b"{\"retention_timeout\": 600, \"visibility_timeout\": 30, \"message_delay\": 5, \"message_deduplication\": false}".to_vec(),
            );
            assert_eq!(StatusCode::from(Status::Created), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                b"{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false}".to_vec(),
            );
        }
        {
            let mut response = run_handler_with(
                create_handler,
                &source,
                b"{\"retention_timeout\": 600, \"visibility_timeout\": 60, \"message_delay\": 5, \"message_deduplication\": false}".to_vec(),
            );
            assert_eq!(StatusCode::from(Status::Conflict), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.len(), 0);
        }
        let get_handler = router.route(&Method::GET, vec!["queues", "my-queue"].into_iter());
        assert!(get_handler.is_some());
        let get_handler = get_handler.unwrap();
        {
            let mut response = run_handler(get_handler.clone(), &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                b"{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false,\"status\":{\"messages\":0,\"visible_messages\":0,\"oldest_message_age\":0}}"
                    .to_vec(),
            );
        }
        let list_handler = router.route(&Method::GET, vec!["queues"].into_iter());
        assert!(list_handler.is_some());
        let list_handler = list_handler.unwrap();
        {
            let mut response = run_handler(list_handler, &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                b"{\"queues\":[{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":600,\"visibility_timeout\":30,\"message_delay\":5,\"message_deduplication\":false}],\"total\":1}"
                    .to_vec(),
            );
        }
        let update_handler = router.route(&Method::POST, vec!["queues", "my-queue"].into_iter());
        assert!(update_handler.is_some());
        let update_handler = update_handler.unwrap();
        {
            let mut response = run_handler_with(
                update_handler,
                &source,
                b"{\"retention_timeout\": 30, \"visibility_timeout\": 10, \"message_delay\": 2, \"message_deduplication\": true}".to_vec(),
            );
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                b"{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":30,\"visibility_timeout\":10,\"message_delay\":2,\"message_deduplication\":true}".to_vec(),
            );
        }
        let delete_handler = router.route(&Method::DELETE, vec!["queues", "my-queue"].into_iter());
        assert!(delete_handler.is_some());
        let delete_handler = delete_handler.unwrap();
        {
            let mut response = run_handler(delete_handler, &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(
                body,
                b"{\"name\":\"my-queue\",\"redrive_policy\":null,\"retention_timeout\":30,\"visibility_timeout\":10,\"message_delay\":2,\"message_deduplication\":true}".to_vec(),
            );
        }
        {
            let mut response = run_handler(get_handler, &source);
            assert_eq!(StatusCode::from(Status::NotFound), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.len(), 0);
        }
    }

    #[test]
    fn messages_router() {
        let source = TestRepoSource::new();
        source
            .get()
            .unwrap()
            .insert_queue(&QueueInput {
                name:                        "my-queue",
                max_receives:                None,
                dead_letter_queue:           None,
                retention_timeout:           100,
                visibility_timeout:          10,
                message_delay:               0,
                content_based_deduplication: false,
            })
            .unwrap()
            .unwrap();
        let router = make_router::<TestRepo, &TestRepoSource>();
        let publish_handler = router.route(&Method::POST, vec!["messages", "my-queue"].into_iter());
        assert!(publish_handler.is_some());
        let publish_handler = publish_handler.unwrap();
        {
            let mut response = run_handler_with(publish_handler, &source, b"{\"content\": \"my message\"}".to_vec());
            assert_eq!(StatusCode::from(Status::Created), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.len(), 0);
        }
        let receive_handler = router.route(&Method::GET, vec!["messages", "my-queue"].into_iter());
        assert!(receive_handler.is_some());
        let receive_handler = receive_handler.unwrap();
        let message_id = {
            let mut response = run_handler(receive_handler, &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.as_slice(), b"{\"content\": \"my message\"}");
            let response_message_id = MessageIdHeader::get(response.headers());
            assert!(!response_message_id.is_empty());
            response_message_id
        };
        {
            let delete_handler = router.route(&Method::DELETE, vec!["messages", &message_id].into_iter());
            assert!(delete_handler.is_some());
            let delete_handler = delete_handler.unwrap();
            let mut response = run_handler(delete_handler, &source);
            assert_eq!(StatusCode::from(Status::Ok), response.status());
            let body = read_body(response.body_mut());
            assert_eq!(body.len(), 0);
        }
    }
}
