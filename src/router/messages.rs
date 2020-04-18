use async_trait::async_trait;
use hyper::{Body, Request, Response};

use crate::{
    models::{message::MessageRepository, queue::QueueRepository},
    router::Handler,
    routes::messages::{delete_message, publish_messages, receive_messages, MaxWaitTime, MessageCount, Source},
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
            let header_value = req
                .headers()
                .get("x-mqs-max-messages")
                .map_or_else(|| None, |v| v.to_str().map_or_else(|_| None, |s| Some(s)));
            if let Some(max_messages) = header_value {
                match max_messages.parse() {
                    Err(_) => Err(()),
                    Ok(n) => {
                        if n > 0 && n < 1000 {
                            Ok(MessageCount(n))
                        } else {
                            Err(())
                        }
                    },
                }
            } else {
                Ok(MessageCount(1))
            }
        };
        let max_wait_time = {
            let header_value = req
                .headers()
                .get("x-mqs-max-wait-time")
                .map_or_else(|| None, |v| v.to_str().map_or_else(|_| None, |s| Some(s)));
            if let Some(max_wait_time) = header_value {
                match max_wait_time.parse() {
                    Err(_) => Err(()),
                    Ok(n) => {
                        if n > 0 && n < 20 {
                            Ok(Some(MaxWaitTime(n)))
                        } else {
                            Err(())
                        }
                    },
                }
            } else {
                Ok(None)
            }
        };
        receive_messages(repo, repo_source, &self.queue_name, message_count, max_wait_time)
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
        publish_messages(repo, &self.queue_name, body.as_slice(), parts.headers)
            .await
            .into_response()
    }
}

#[async_trait]
impl<R: MessageRepository, S: Send> Handler<(R, S)> for DeleteMessageHandler {
    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        delete_message(repo, &self.message_id).into_response()
    }
}
