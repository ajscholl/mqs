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
extern crate diesel;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod connection;
pub mod models;
pub mod router;
pub mod routes;
pub(crate) mod schema;
pub(crate) mod wait;
