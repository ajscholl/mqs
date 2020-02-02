#![feature(proc_macro_hygiene, decl_macro)]

extern crate base64;
extern crate chrono;
extern crate hyper;
extern crate regex;
extern crate time;
extern crate uuid;

#[macro_use] extern crate diesel;
#[macro_use] extern crate log;
#[macro_use] extern crate rocket;
#[macro_use] extern crate serde_derive;

pub mod client;
pub mod connection;
pub mod models;
pub mod multipart;
pub mod routes;
pub mod schema;
