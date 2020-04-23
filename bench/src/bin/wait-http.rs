#![feature(async_closure)]

#[macro_use]
extern crate log;
extern crate dotenv;

use cached::once_cell::sync::Lazy;
use dotenv::dotenv;
use log::Level;
use std::{env, io::Stdout, ops::Deref, thread::sleep, time::Duration};
use tokio::runtime::Builder;

use mqs_client::Service;
use mqs_server::logger::json::Logger;

fn get_service() -> Service {
    let host = env::var("MQS_SERVER").unwrap_or("localhost".to_string());
    Service::new(&format!("http://{}:7843", &host))
}

fn main() {
    dotenv().ok();

    static LOGGER: Lazy<Logger<Stdout>> = Lazy::new(|| Logger::new(Level::Debug, std::io::stdout()));
    log::set_logger(LOGGER.deref())
        .map(|()| log::set_max_level(LOGGER.level().to_level_filter()))
        .unwrap();

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
