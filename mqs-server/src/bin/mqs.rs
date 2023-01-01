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

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use std::{env, env::VarError, sync::Arc};

use mqs_common::{
    connection::{Pool, Source},
    router::{handle, Router},
    server,
    server::ServerHandler,
};
use mqs_server::{make_router, PgRepository};

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
        self.pool.try_get().map(PgRepository::new)
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
}

#[async_trait]
impl ServerHandler for HandlerService {
    async fn handle(&self, req: Request<Body>) -> Response<Body> {
        let repo = self.pool.get().ok().map(PgRepository::new);
        handle(
            repo,
            RepoSource::new(Arc::clone(&self.pool)),
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
    server::run(
        |pool| HandlerService::new(pool, make_router(), get_max_message_size()),
        7843,
    );
}
