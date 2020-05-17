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

//! Tool to wait until a connection to the database can be established. Used in CI tests.

#[macro_use]
extern crate log;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use log::Level;
use std::{io::Stdout, thread::sleep, time::Duration};

use mqs_common::logger::{configure_logger, json::Logger, NewJsonLogger};
use mqs_server::connection::init_pool_maybe;

fn main() {
    static LOGGER: Lazy<Logger<Stdout>, NewJsonLogger> = Lazy::new(NewJsonLogger::new(Level::Debug));

    dotenv().ok();
    configure_logger(&*LOGGER);

    loop {
        if let Err(error) = init_pool_maybe() {
            info!("Failed to connect to database, waiting... {}", error);
            sleep(Duration::from_secs(5));
        } else {
            info!("Database connection successfully established");
            return;
        }
    }
}
