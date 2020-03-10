use hyper::{Response, Request, Body};

use crate::router::Handler;
use crate::routes::messages::{delete_message, receive_messages, MessageCount, publish_messages};
use crate::models::message::MessageRepository;
use crate::models::queue::QueueRepository;

pub struct ReceiveMessagesHandler {
    pub queue_name: String,
}

pub struct PublishMessagesHandler {
    pub queue_name: String,
}

pub struct DeleteMessageHandler {
    pub message_id: String,
}

impl <R: MessageRepository + QueueRepository> Handler<R> for ReceiveMessagesHandler {
    fn handle(&self, repo: R, req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        let message_count = {
            let header_value = req
                .headers()
                .get("X-MQS-MAX-MESSAGES")
                .map_or_else(|| None, |v| v
                    .to_str()
                    .map_or_else(|_| None, |s| Some(s)));
            if let Some(max_messages) = header_value {
                match max_messages.parse() {
                    Err(_) => Err(()),
                    Ok(n) => if n > 0 && n < 1000 {
                        Ok(MessageCount(n))
                    } else {
                        Err(())
                    },
                }
            } else {
                Ok(MessageCount(1))
            }
        };
        receive_messages(repo, &self.queue_name, message_count).into_response()
    }
}

impl <R: MessageRepository + QueueRepository> Handler<R> for PublishMessagesHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, repo: R, req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let (parts, _) = req.into_parts();
        publish_messages(repo, &self.queue_name, body.as_slice(), parts.headers).into_response()
    }
}

impl <R: MessageRepository> Handler<R> for DeleteMessageHandler {
    fn handle(&self, repo: R, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        delete_message(repo, &self.message_id).into_response()
    }
}
