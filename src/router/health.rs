use hyper::{Response, Request, Body};

use crate::router::Handler;
use crate::models::health::HealthCheckRepository;

pub struct HealthHandler;

impl <R: HealthCheckRepository> Handler<R> for HealthHandler {
    fn handle(&self, repo: R, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        Response::new(Body::from(if repo.check_health() {
            "green"
        } else {
            "red"
        }))
    }
}
