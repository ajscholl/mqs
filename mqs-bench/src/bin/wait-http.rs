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

//! Tool to wait until a connection to the MQS server can be established. Used in CI tests.

#[macro_use]
extern crate log;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use log::Level;
use std::{env, io::Stdout, ops::Deref, thread::sleep, time::Duration};
use tokio::runtime::Builder;

use mqs_client::Service;
use mqs_common::logger::{configure_logger, json::Logger, NewJsonLogger};

fn get_service() -> Service {
    let host = env::var("MQS_SERVER").unwrap_or("localhost".to_string());
    Service::new(&format!("http://{}:7843", &host))
}

fn main() {
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>, NewJsonLogger> = Lazy::new(NewJsonLogger::new(Level::Debug));
    configure_logger(LOGGER.deref());

    let mut rt = Builder::new().enable_all().threaded_scheduler().build().unwrap();

    rt.block_on(async {
        loop {
            let s = get_service();
            match s.check_health().await {
                Err(error) => {
                    info!("Failed to connect to service, waiting... {}", error);
                    sleep(Duration::from_secs(5));
                },
                Ok(true) => {
                    info!("Service connection successfully established");
                    return;
                },
                Ok(false) => {
                    info!("Service not yet healthy, waiting...");
                    sleep(Duration::from_secs(5));
                },
            }
        }
    })
}
