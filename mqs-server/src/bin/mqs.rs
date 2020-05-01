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
use std::{
    convert::Infallible,
    env,
    env::VarError,
    io::Stdout,
    net::SocketAddr,
    ops::Deref,
    sync::Arc,
    time::Duration,
};
use tokio::{runtime::Builder, time::delay_for};

use mqs_common::{
    logger::{
        json::Logger,
        trace_id::{create_trace_id, with_trace_id},
    },
    router::{handler::handle, Router},
};
use mqs_server::{
    connection::{init_pool, DbConn, Pool},
    models::PgRepository,
    router::make_router,
    routes::messages::Source,
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
    fn new(pool: Arc<Pool>) -> Self {
        RepoSource { pool }
    }
}

impl Source<PgRepository> for RepoSource {
    fn get(&self) -> Option<PgRepository> {
        let conn = self.pool.try_get()?;
        Some(PgRepository::new(DbConn(conn)))
    }
}

impl HandlerService {
    fn new(pool: Pool, router: Router<(PgRepository, RepoSource)>, max_message_size: usize) -> Self {
        HandlerService {
            pool: Arc::new(pool),
            router,
            max_message_size,
        }
    }

    async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let conn = self.pool.get().map_or_else(|_| None, |conn| Some(DbConn(conn)));
        let repo = match conn {
            None => None,
            Some(conn) => Some(PgRepository::new(conn)),
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
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>> = Lazy::new(|| Logger::new(Level::Info, std::io::stdout()));
    log::set_logger(LOGGER.deref())
        .map(|()| log::set_max_level(LOGGER.level().to_level_filter()))
        .unwrap();

    let (pool, pool_size) = init_pool();
    let mut rt = Builder::new()
        .enable_all()
        .threaded_scheduler()
        .core_threads(pool_size as usize)
        .build()
        .unwrap();
    let service = Arc::new(HandlerService::new(pool, make_router(), get_max_message_size()));

    rt.spawn(async {
        loop {
            delay_for(Duration::from_secs(10)).await;
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
                    async move { with_trace_id(id, req_service.handle(req)).await }
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
    })
}
