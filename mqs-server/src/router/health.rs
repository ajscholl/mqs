use async_trait::async_trait;
use hyper::{Body, Request, Response};
use mqs_common::router;

use crate::models::health::HealthCheckRepository;

pub struct Handler;

#[async_trait]
impl<R: HealthCheckRepository, S: Send> router::Handler<(R, S)> for Handler {
    async fn handle(&self, (mut repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        Response::new(Body::from(if repo.check_health() { "green" } else { "red" }))
    }
}
