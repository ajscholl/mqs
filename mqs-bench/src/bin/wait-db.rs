#![warn(
    rust_2018_idioms,
    future_incompatible,
    missing_copy_implementations,
    trivial_numeric_casts,
    unsafe_code,
    unused,
    unused_qualifications,
    variant_size_differences
)]

#[macro_use]
extern crate log;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use log::Level;
use std::{io::Stdout, ops::Deref, thread::sleep, time::Duration};

use mqs_common::logger::{configure_logger, json::Logger, NewJsonLogger};
use mqs_server::connection::init_pool_maybe;

fn main() {
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>, NewJsonLogger> = Lazy::new(NewJsonLogger::new(Level::Debug));
    configure_logger(LOGGER.deref());

    loop {
        match init_pool_maybe() {
            Err(error) => {
                info!("Failed to connect to database, waiting... {}", error);
                sleep(Duration::from_secs(5));
            },
            Ok(_) => {
                info!("Database connection successfully established");
                return;
            },
        }
    }
}
