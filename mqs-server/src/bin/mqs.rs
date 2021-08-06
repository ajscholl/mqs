#![warn(
    missing_docs,
    rust_2018_idioms,
    future_incompatible,
    missing_copy_implementations,
    trivial_numeric_casts,
    unsafe_code,
    unused,
    unused_qualifications,
    variant_size_differences
)]
#![cfg_attr(test, deny(warnings))]

//! MQS server binary.

#[macro_use]
extern crate log;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use hyper::{
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body,
    Request,
    Response,
    Server,
};
use log::{Level, Log};
use std::{convert::Infallible, env, env::VarError, io::Stdout, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{runtime::Builder, time::sleep};

use mqs_common::{
    logger::{configure_logger, create_trace_id, json::Logger, with_trace_id, NewJsonLogger},
    router::{handle, Router},
};
use mqs_server::{
    connection::{init_pool_maybe, Pool, Source},
    make_router,
    PgRepository,
};

struct HandlerService {
    pool:             Arc<Pool>,
    router:           Router<(PgRepository, RepoSource)>,
    max_message_size: usize,
}

struct RepoSource {
    pool: Arc<Pool>,
}

impl RepoSource {
    const fn new(pool: Arc<Pool>) -> Self {
        Self { pool }
    }
}

impl Source<PgRepository> for RepoSource {
    fn get(&self) -> Option<PgRepository> {
        let conn = self.pool.try_get()?;
        Some(PgRepository::new(conn))
    }
}

impl HandlerService {
    fn new(pool: Pool, router: Router<(PgRepository, RepoSource)>, max_message_size: usize) -> Self {
        Self {
            pool: Arc::new(pool),
            router,
            max_message_size,
        }
    }

    async fn handle(&self, req: Request<Body>) -> Response<Body> {
        let repo = match self.pool.get() {
            Err(_) => None,
            Ok(conn) => Some(PgRepository::new(conn)),
        };
        handle(
            repo,
            RepoSource::new(self.pool.clone()),
            &self.router,
            self.max_message_size,
            req,
        )
        .await
    }
}

fn get_max_message_size() -> usize {
    const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;
    match env::var("MAX_MESSAGE_SIZE") {
        Err(VarError::NotPresent) => DEFAULT_MAX_MESSAGE_SIZE,
        Err(VarError::NotUnicode(_)) => {
            panic!("MAX_MESSAGE_SIZE has to be a valid unicode string (it should be a numeric string in fact)")
        },
        Ok(s) => match s.parse::<usize>() {
            Err(err) => panic!("Failed to parse maximum message size '{}': {}", s, err),
            Ok(n) => {
                if n < 1024 {
                    panic!("Maximum message size must be at least 1024, got {}", n)
                } else {
                    n
                }
            },
        },
    }
}

fn main() {
    static LOGGER: Lazy<Logger<Stdout>, NewJsonLogger> = Lazy::new(NewJsonLogger::new(Level::Info));

    dotenv().ok();
    configure_logger(&*LOGGER);

    let (pool, pool_size) = init_pool_maybe().expect("Failed to initialize database pool");
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(pool_size as usize)
        .build()
        .unwrap();
    let service = Arc::new(HandlerService::new(pool, make_router(), get_max_message_size()));

    rt.spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
            LOGGER.flush();
        }
    });

    rt.block_on(async {
        // Setup and configure server...
        let addr = SocketAddr::from(([0, 0, 0, 0], 7843));
        let make_service = make_service_fn(move |conn: &AddrStream| {
            let remote_addr = conn.remote_addr();
            info!("New connection from {}", remote_addr);
            let conn_service = Arc::clone(&service);
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let req_service = Arc::clone(&conn_service);
                    let id = create_trace_id(&req);
                    async move { Ok::<_, Infallible>(with_trace_id(id, req_service.handle(req)).await) }
                }))
            }
        });

        let server = Server::bind(&addr).http1_keepalive(true).serve(make_service);

        info!("Started server on {} with a pool of size {}", addr, pool_size);

        // And run forever...
        if let Err(e) = server.await {
            error!("server error: {}", e);
        }

        info!("Shutting down server");
        LOGGER.flush();
    });
}
