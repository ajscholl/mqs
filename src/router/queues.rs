use async_trait::async_trait;
use hyper::{Body, Request, Response};

use crate::{
    models::queue::QueueRepository,
    router::Handler,
    routes::queues::{delete_queue, describe_queue, list_queues, new_queue, update_queue, QueuesRange},
};

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

#[async_trait]
impl<R: QueueRepository, S: Send> Handler<(R, S)> for DescribeQueueHandler {
    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        describe_queue(repo, &self.queue_name).into_response()
    }
}

#[async_trait]
impl<R: QueueRepository, S: Send> Handler<(R, S)> for CreateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        let params = serde_json::from_slice(body.as_slice());
        new_queue(repo, &self.queue_name, params).into_response()
    }
}

#[async_trait]
impl<R: QueueRepository, S: Send> Handler<(R, S)> for UpdateQueueHandler {
    fn needs_body(&self) -> bool {
        true
    }

    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        let params = serde_json::from_slice(body.as_slice());
        update_queue(repo, &self.queue_name, params).into_response()
    }
}

#[async_trait]
impl<R: QueueRepository, S: Send> Handler<(R, S)> for DeleteQueueHandler {
    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        delete_queue(repo, &self.queue_name).into_response()
    }
}

#[async_trait]
impl<R: QueueRepository, S: Send> Handler<(R, S)> for ListQueuesHandler {
    async fn handle(&self, (repo, _): (R, S), req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        let range = QueuesRange::from_hyper(req);
        list_queues(repo, range).into_response()
    }
}
