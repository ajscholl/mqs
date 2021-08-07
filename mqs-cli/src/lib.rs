#![feature(stmt_expr_attributes)]

#[macro_use]
extern crate serde_derive;

mod args;
mod help;
mod run;

pub use args::{parse_os_args, ParsedArgs};
pub use help::{show_help, show_subcommand_help};
pub use run::run_command;
