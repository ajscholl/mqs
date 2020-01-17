#![feature(proc_macro_hygiene, decl_macro)]

extern crate uuid;
extern crate jsonwebtoken;
extern crate base64;
extern crate regex;
extern crate chrono;
extern crate rand;

#[macro_use] extern crate log;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate rocket;
#[macro_use] extern crate diesel;
#[macro_use] extern crate serde_derive;

pub mod models;
pub mod schema;
pub mod connection;
pub mod routes;
