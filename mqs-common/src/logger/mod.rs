use log::Level;

use crate::logger::json::Logger;
use std::{
    borrow::Borrow,
    env,
    io::{stdout, Stdout, Write},
};

pub mod json;
pub mod trace_id;

#[derive(Clone, Copy)]
pub struct NewJsonLogger {
    default_log_level: Level,
}

impl NewJsonLogger {
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

pub fn configure_logger<W: Write + Send>(logger: &'static Logger<W>) {
    log::set_logger(logger)
        .map(|()| log::set_max_level(logger.level().to_level_filter()))
        .unwrap();
}
