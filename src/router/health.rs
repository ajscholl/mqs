use crate::router::Handler;
use hyper::{Response, Request, Body};
use crate::models::health::Health;
use crate::connection::DbConn;

pub struct HealthHandler;

impl Handler<DbConn> for HealthHandler {
    fn handle(&self, conn: DbConn, _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
        Response::new(Body::from(if Health::check(&conn) {
            "green"
        } else {
            "red"
        }))
    }
}
