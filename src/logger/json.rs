use log::{Log, Metadata, Record, Level};
use chrono::{Utc, DateTime};
use std::io::Write;

#[derive(Serialize, Debug)]
struct LogMessage<'a> {
    timestamp: DateTime<Utc>,
    level: String,
    level_num: i32,
    target: &'a str,
    module_path: Option<&'a str>,
    file: Option<&'a str>,
    line: Option<u32>,
    message: String,
}

impl<'a> LogMessage<'a> {
    fn build(record: &Record<'a>) -> Self {
        LogMessage {
            timestamp: Utc::now(),
            level: record.level().to_string(),
            level_num: record.level() as i32,
            target: record.target(),
            module_path: record.module_path(),
            file: record.file(),
            line: record.line(),
            message: format!("{:?}", record.args()),
        }
    }
}

pub struct Logger {
    pub level: Level,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let msg = LogMessage::build(record);
            let str = serde_json::to_string(&msg).unwrap();
            let mut stdout = std::io::stdout();
            stdout.write_all(str.as_bytes()).unwrap();
            stdout.write_all([10u8].as_ref()).unwrap()
        }
    }

    fn flush(&self) {
        std::io::stdout().flush().unwrap()
    }
}
