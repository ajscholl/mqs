#![feature(async_closure)]
extern crate mqs;

#[macro_use] extern crate log;
extern crate dotenv;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use dotenv::dotenv;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use log::Level;

use mqs::logger::json::Logger;
use mqs::router::Router;
use mqs::router::handler::{handle, make_router};
use mqs::connection::{init_pool, Pool, DbConn};
use tokio::runtime::Builder;
use std::io::Stdout;
use cached::once_cell::sync::Lazy;
use std::ops::Deref;

struct HandlerService {
    pool: Pool,
    router: Router<DbConn>,
}

impl HandlerService {
    async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let conn = self.pool.get().map_or_else(|_| None, |conn| Some(DbConn(conn)));
        handle(conn, &self.router, req).await
    }
}

fn main() {
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>> = Lazy::new(|| Logger::new(Level::Info, std::io::stdout()));
    log::set_logger(LOGGER.deref())
        .map(|()| log::set_max_level(LOGGER.level().to_level_filter())).unwrap();

    let (pool, pool_size) = init_pool();
    let mut rt = Builder::new()
        .enable_all()
        .threaded_scheduler()
        .core_threads(pool_size as usize)
        .build()
        .unwrap();

    rt.block_on(async {
        // Setup and configure server...
        let addr = SocketAddr::from(([0, 0, 0, 0], 7843));
        let service = Arc::new(HandlerService { pool: pool, router: make_router() });
        let make_service = make_service_fn(move |conn: &AddrStream| {
            let remote_addr = conn.remote_addr();
            info!("New connection from {}", remote_addr);
            let conn_service = Arc::clone(&service);
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let req_service = Arc::clone(&conn_service);
                    async move {
                        req_service.handle(req).await
                    }
                }))
            }
        });

        let server = Server::bind(&addr)
            .http1_keepalive(true)
            .serve(make_service);

        info!("Started server on {} with a pool of size {}", addr, pool_size);

        // And run forever...
        if let Err(e) = server.await {
            error!("server error: {}", e);
        }
    })
}
