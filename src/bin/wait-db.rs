#![feature(async_closure)]
extern crate mqs;

#[macro_use]
extern crate log;
extern crate dotenv;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use log::Level;
use std::{io::Stdout, ops::Deref, thread::sleep, time::Duration};

use mqs::{connection::init_pool_maybe, logger::json::Logger};

fn main() {
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>> = Lazy::new(|| Logger::new(Level::Debug, std::io::stdout()));
    log::set_logger(LOGGER.deref())
        .map(|()| log::set_max_level(LOGGER.level().to_level_filter()))
        .unwrap();

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
