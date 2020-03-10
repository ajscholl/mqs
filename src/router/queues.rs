use crate::router::Handler;
use hyper::{Response, Request, Body};

use crate::routes::queues::{describe_queue, delete_queue, new_queue, update_queue, list_queues, QueuesRange};
use crate::models::queue::QueueRepository;

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

impl <R: QueueRepository> Handler<R> for DescribeQueueHandler {
    fn handle(&self, repo: R, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        describe_queue(repo, &self.queue_name).into_response()
    }
}

impl <R: QueueRepository> Handler<R> for CreateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, repo: R, _req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let params = serde_json::from_slice(body.as_slice());
        new_queue(repo, &self.queue_name, params).into_response()
    }
}

impl <R: QueueRepository> Handler<R> for UpdateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    fn handle(&self, repo: R, _req: Request<Body>, body: Vec<u8>) -> Response<Body> {
        let params = serde_json::from_slice(body.as_slice());
        update_queue(repo, &self.queue_name, params).into_response()
    }
}

impl <R: QueueRepository> Handler<R> for DeleteQueueHandler {
    fn handle(&self, repo: R, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        delete_queue(repo, &self.queue_name).into_response()
    }
}

impl <R: QueueRepository> Handler<R> for ListQueuesHandler {
    fn handle(&self, repo: R, req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        let range = QueuesRange::from_hyper(req);
        list_queues(repo, range).into_response()
    }
}
