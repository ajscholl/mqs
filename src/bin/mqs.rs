#![feature(proc_macro_hygiene, decl_macro)]
extern crate mqs;

#[macro_use] extern crate log;
#[macro_use] extern crate rocket;
extern crate dotenv;

use dotenv::dotenv;
use std::env;
use std::env::VarError;
use rocket::Config;
use rocket::config::{Environment, Limits};
use rocket::logger::LoggingLevel;

use mqs::connection::init_pool;
use mqs::routes::*;

fn main() {
    dotenv().ok();

    let run_env = match env::var("ENV") {
        Err(VarError::NotPresent) => {
            info!("No environment given, defaulting to development");
            Environment::Development
        },
        Err(err) => {
            error!("Failed to get ENV variable: {}, defaulting to development", err);
            Environment::Development
        },
        Ok(name) => {
            if name == "prod" || name == "production" {
                Environment::Production
            } else if name == "staging" || name == "sandbox" {
                Environment::Staging
            } else if name == "dev" || name == "development" {
                Environment::Development
            } else {
                info!("Unknown environment {} given, expecting prod, staging or dev, defaulting to development", name);
                Environment::Development
            }
        },
    };

    let (pool, pool_size) = init_pool();

    let config = Config::build(run_env)
        .address("0.0.0.0")
        .port(7843)
        .keep_alive(0)
        .workers(pool_size)
        .log_level(LoggingLevel::Normal)
        .limits(Limits::new().limit("forms", 1024 * 1024).limit("json", 1024 * 1024))
        .finalize()
        .expect("Unwrapping server config");

    rocket::custom(config)
        .manage(pool)
        .mount("/", routes![health::health])
        .mount("/", routes![queues::new_queue])
        .mount("/", routes![queues::update_queue])
        .mount("/", routes![queues::delete_queue])
        .mount("/", routes![queues::list_queues])
        .mount("/", routes![queues::describe_queue])
        .mount("/", routes![messages::publish_messages])
        .mount("/", routes![messages::receive_messages])
        .mount("/", routes![messages::delete_message])
        .launch();
}
