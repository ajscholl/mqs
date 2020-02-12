use crate::router::Handler;
use hyper::{Response, Request, Body};
use crate::connection::DbConn;
use crate::routes::queues::{describe_queue, delete_queue, new_queue, update_queue, list_queues, QueuesRange};

pub struct DescribeQueueHandler {
    pub queue_name: String,
}

pub struct CreateQueueHandler {
    pub queue_name: String,
}

pub struct UpdateQueueHandler {
    pub queue_name: String,
}

pub struct DeleteQueueHandler {
    pub queue_name: String,
}

pub struct ListQueuesHandler;

impl Handler<DbConn> for DescribeQueueHandler {
    fn handle(&self, conn: DbConn, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        describe_queue(conn, &self.queue_name).into_response()
    }
}

impl Handler<DbConn> for CreateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, conn: DbConn, _req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let params = serde_json::from_slice(body.as_slice());
        new_queue(conn, &self.queue_name, params).into_response()
    }
}

impl Handler<DbConn> for UpdateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, conn: DbConn, _req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let params = serde_json::from_slice(body.as_slice());
        update_queue(conn, &self.queue_name, params).into_response()
    }
}

impl Handler<DbConn> for DeleteQueueHandler {
    fn handle(&self, conn: DbConn, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        delete_queue(conn, &self.queue_name).into_response()
    }
}

impl Handler<DbConn> for ListQueuesHandler {
    fn handle(&self, conn: DbConn, req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        let range = QueuesRange::from_hyper(req);
        list_queues(conn, range).into_response()
    }
}
