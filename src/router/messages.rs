use crate::router::Handler;
use hyper::{Response, Request, Body};
use crate::connection::DbConn;
use crate::routes::messages::{delete_message, receive_messages, MessageCount, publish_messages, MessageContentType};

pub struct ReceiveMessagesHandler {
    pub queue_name: String,
}

pub struct PublishMessagesHandler {
    pub queue_name: String,
}

pub struct DeleteMessageHandler {
    pub message_id: String,
}

impl Handler<DbConn> for ReceiveMessagesHandler {
    fn handle(&self, conn: DbConn, req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
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
        receive_messages(conn, &self.queue_name, message_count).into_response()
    }
}

impl Handler<DbConn> for PublishMessagesHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, conn: DbConn, req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let content_type = MessageContentType::from_hyper(&req);

        publish_messages(conn, &self.queue_name, body.as_slice(), content_type).into_response()
    }
}

impl Handler<DbConn> for DeleteMessageHandler {
    fn handle(&self, conn: DbConn, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        delete_message(conn, &self.message_id).into_response()
    }
}
