#![feature(stmt_expr_attributes)]
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
extern crate serde_derive;

mod args;
mod help;
mod run;

pub use args::{parse_os_args, ParsedArgs};
pub use help::{show_help, show_subcommand_help};
pub use run::run_command;
