use async_trait::async_trait;
use hyper::{Body, Request, Response};

use crate::{models::health::HealthCheckRepository, router::Handler};

pub struct HealthHandler;

#[async_trait]
impl<R: HealthCheckRepository, S: Send> Handler<(R, S)> for HealthHandler {
    async fn handle(&self, (repo, _): (R, S), _req: Request<Body>, _body: Vec<u8>) -> Response<Body>
    where
        R: 'async_trait,
        S: 'async_trait,
    {
        Response::new(Body::from(if repo.check_health() { "green" } else { "red" }))
    }
}
