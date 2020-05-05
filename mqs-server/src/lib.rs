#![feature(in_band_lifetimes)]
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

//! Server definitions and implementation.

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

/// Utilities to connect to the database.
pub mod connection;
pub(crate) mod models;
pub(crate) mod router;
pub(crate) mod routes;
pub(crate) mod schema;
pub(crate) mod wait;

pub use models::PgRepository;
pub use router::make_router;
