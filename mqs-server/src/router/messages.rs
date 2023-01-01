use async_trait::async_trait;
use hyper::{header::HeaderName, Body, Request, Response};
use mqs_common::{connection::Source, get_header, router::Handler};

use crate::{
    models::{message::MessageRepository, queue::QueueRepository},
    routes::messages::{delete, publish, receive, MaxWaitTime, MessageCount},
};

pub struct ReceiveMessagesHandler {
    pub queue_name: String,
}

pub struct PublishMessagesHandler {
    pub queue_name: String,
}

pub struct DeleteMessageHandler {
    pub message_id: String,
}

#[async_trait]
impl<R: MessageRepository + QueueRepository, S: Source<R>> Handler<(R, S)> for ReceiveMessagesHandler {
    async fn handle(&self, (repo, repo_source): (R, S), req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        let message_count = {
            let header_value = get_header(req.headers(), HeaderName::from_static("x-mqs-max-messages"));
            header_value.map_or(Ok(MessageCount(1)), |max_messages| {
                max_messages.parse().map_or(Err(()), |n| {
                    if n > 0 && n < 1000 {
                        Ok(MessageCount(n))
                    } else {
                        Err(())
                    }
                })
            })
        };
        let max_wait_time = {
            let header_value = get_header(req.headers(), HeaderName::from_static("x-mqs-max-wait-time"));
            header_value.map_or(Ok(None), |max_wait_time| {
                max_wait_time.parse().map_or(Err(()), |n| {
                    if n > 0 && n < 20 {
                        Ok(Some(MaxWaitTime(n)))
                    } else {
                        Err(())
                    }
                })
            })
        };
        receive(repo, repo_source, &self.queue_name, message_count, max_wait_time)
            .await
            .into_response()
    }
}

#[async_trait]
impl<R: MessageRepository + QueueRepository, S: Send> Handler<(R, S)> for PublishMessagesHandler {
    fn needs_body(&self) -> bool {
        true
    }

    async fn handle(&self, (repo, _): (R, S), req: Request<Body>, body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        let (parts, _) = req.into_parts();
        publish(repo, &self.queue_name, body.as_slice(), parts.headers)
            .await
            .into_response()
    }
}

#[async_trait]
impl<R: MessageRepository, S: Send> Handler<(R, S)> for DeleteMessageHandler {
    async fn handle(&self, (mut repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        delete(&mut repo, &self.message_id).into_response()
    }
}
