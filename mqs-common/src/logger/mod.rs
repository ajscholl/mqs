use log::Level;

use crate::logger::json::Logger;
use std::{
    borrow::Borrow,
    env,
    io::{stdout, Stdout, Write},
};

/// A logger implementation which writes each log messages as a json encoded object.
pub mod json;
mod trace_id;

pub use trace_id::*;

/// A function which creates a new json logger. It will look up the 'LOG_LEVEL' environment variable
/// and use that (if it is set to any of 'trace', 'debug', 'info', 'warn', or 'error') as the log
/// level. Otherwise it will fall back to the default log level specified in `new`.
#[derive(Clone, Copy)]
pub struct NewJsonLogger {
    default_log_level: Level,
}

impl NewJsonLogger {
    /// Create a factory function for a json logger. The function will use the given
    /// log level as default if no other level is specified in the environment.
    pub const fn new(default_log_level: Level) -> Self {
        NewJsonLogger { default_log_level }
    }
}

impl<Args> FnOnce<Args> for NewJsonLogger {
    type Output = Logger<Stdout>;

    extern "rust-call" fn call_once(self, args: Args) -> Self::Output {
        self.call(args)
    }
}

impl<Args> Fn<Args> for NewJsonLogger {
    extern "rust-call" fn call(&self, _args: Args) -> Self::Output {
        let w = stdout();
        let l = match env::var("LOG_LEVEL") {
            Err(_) => self.default_log_level,
            Ok(s) => match s.borrow() {
                "trace" => Level::Trace,
                "debug" => Level::Debug,
                "info" => Level::Info,
                "warn" => Level::Warn,
                "error" => Level::Error,
                _ => self.default_log_level,
            },
        };

        Logger::new(l, w)
    }
}

impl<Args> FnMut<Args> for NewJsonLogger {
    extern "rust-call" fn call_mut(&mut self, args: Args) -> Self::Output {
        self.call(args)
    }
}

/// Set the given json logger as the current logger and set the log level to the level specified
/// by the json logger.
pub fn configure_logger<W: Write + Send>(logger: &'static Logger<W>) {
    log::set_logger(logger)
        .map(|()| log::set_max_level(logger.level().to_level_filter()))
        .unwrap();
}
