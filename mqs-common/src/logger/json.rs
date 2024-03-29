use log::{Level, Log, Metadata, Record};
use std::{
    cell::Cell,
    io::{BufWriter, Write},
    sync::Mutex,
};

use crate::{logger::get_trace_id, UtcTime};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct LogMessage<'a> {
    timestamp:   UtcTime,
    level:       String,
    level_num:   i32,
    target:      &'a str,
    module_path: Option<&'a str>,
    file:        Option<&'a str>,
    line:        Option<u32>,
    trace_id:    Option<String>,
    message:     String,
}

impl<'a> LogMessage<'a> {
    fn build(record: &Record<'a>) -> Self {
        LogMessage {
            timestamp:   UtcTime::now(),
            level:       record.level().to_string(),
            level_num:   record.level() as i32,
            target:      record.target(),
            module_path: record.module_path(),
            file:        record.file(),
            line:        record.line(),
            trace_id:    get_trace_id().map(|id| id.to_string()),
            message:     format!("{:?}", record.args()),
        }
    }
}

/// A json logger which writes each log message as json encoded object on a new line.
pub struct Logger<W: Write> {
    level:  Level,
    writer: Mutex<Cell<BufWriter<W>>>,
}

impl<W: Write> Logger<W> {
    /// Create a new json logger with the given level and writer.
    ///
    /// ```
    /// use log::Level;
    /// use mqs_common::logger::json::Logger;
    /// use std::io::stdout;
    ///
    /// let logger = Logger::new(Level::Info, stdout());
    /// assert_eq!(logger.level(), Level::Info);
    /// ```
    pub fn new(level: Level, writer: W) -> Self {
        Self {
            level,
            writer: Mutex::new(Cell::new(BufWriter::new(writer))),
        }
    }

    /// Get the current level of the logger. This is not the level configured by the logging library,
    /// but an additional filter for messages which are reaching this logger!
    ///
    /// ```
    /// use log::Level;
    /// use mqs_common::logger::json::Logger;
    /// use std::io::stdout;
    ///
    /// let logger = Logger::new(Level::Info, stdout());
    /// assert_eq!(logger.level(), Level::Info);
    /// ```
    pub const fn level(&self) -> Level {
        self.level
    }

    /// Change the current level of this logger. This is not the level configured by the logging library,
    /// but an additional filter for messages which are reaching this logger!
    ///
    /// ```
    /// use log::Level;
    /// use mqs_common::logger::json::Logger;
    /// use std::io::stdout;
    ///
    /// let mut logger = Logger::new(Level::Info, stdout());
    /// assert_eq!(logger.level(), Level::Info);
    /// logger.set_level(Level::Warn);
    /// assert_eq!(logger.level(), Level::Warn);
    /// ```
    pub fn set_level(&mut self, level: Level) {
        self.level = level;
    }
}

impl<W: Write + Send> Log for Logger<W> {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            let msg = LogMessage::build(record);
            if let Ok(mut line) = serde_json::to_vec(&msg) {
                line.push(b'\n');
                if let Ok(mut writer) = self.writer.lock() {
                    // we ignore the result of the call as we can't handle an error here
                    drop(writer.get_mut().write_all(line.as_slice()));
                }
            }
        }
    }

    fn flush(&self) {
        if let Ok(mut writer) = self.writer.lock() {
            // we ignore the result of the call as we can't handle an error here
            drop(writer.get_mut().flush());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::UtcTime;
    use std::io::Error;

    struct TestWriter {
        written: Vec<u8>,
        flushed: bool,
    }

    impl TestWriter {
        const fn new() -> Self {
            TestWriter {
                written: Vec::new(),
                flushed: false,
            }
        }

        fn assert_expectations(&self, mut start_time: UtcTime, expected_messages: Vec<(Level, &str)>) {
            assert!(self.flushed);
            let messages = String::from_utf8(self.written.clone()).unwrap();
            let lines: Vec<&str> = messages.split("\n").collect();
            assert_eq!(lines.len(), expected_messages.len() + 1); // final line ends with '\n', so final element is ""
            assert_eq!(lines[lines.len() - 1], ""); // final line should be empty
            for i in 0..expected_messages.len() {
                let parsed: LogMessage<'_> = serde_json::from_str(lines[i]).unwrap();
                assert!(parsed.timestamp.ge(&start_time));
                start_time = parsed.timestamp.clone(); // time only moves forward
                assert_eq!(parsed, LogMessage {
                    timestamp:   start_time.clone(),
                    level:       expected_messages[i].0.to_string(),
                    level_num:   expected_messages[i].0 as i32,
                    target:      "mqs_common::logger::json::test",
                    module_path: Some("mqs_common::logger::json::test"),
                    file:        Some("mqs-common/src/logger/json.rs"),
                    line:        parsed.line,
                    trace_id:    None,
                    message:     expected_messages[i].1.to_string(),
                });
            }
        }
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
            self.flushed = false;
            for b in buf {
                self.written.push(*b);
            }

            Ok(buf.len())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.flushed = true;

            Ok(())
        }
    }

    fn log(
        logger: &Logger<TestWriter>,
        level: Level,
        message: &str,
        module_path: &'static str,
        file: &'static str,
        line: u32,
    ) {
        logger.log(
            &Record::builder()
                .args(format_args!("{}", message))
                .level(level)
                .target(module_path)
                .module_path_static(Some(module_path))
                .file_static(Some(file))
                .line(Some(line))
                .build(),
        );
    }

    #[test]
    async fn logger_test() {
        let logger = Logger::new(Level::Info, TestWriter::new());
        let start_time = UtcTime::now();

        log(
            &logger,
            Level::Trace,
            "Should not appear",
            module_path!(),
            file!(),
            line!(),
        );
        log(
            &logger,
            Level::Debug,
            "This is also not needed",
            module_path!(),
            file!(),
            line!(),
        );
        log(
            &logger,
            Level::Info,
            "This should get logged",
            module_path!(),
            file!(),
            line!(),
        );
        log(
            &logger,
            Level::Warn,
            "And of course this",
            module_path!(),
            file!(),
            line!(),
        );
        log(
            &logger,
            Level::Error,
            "This has to get logged, otherwise would be bad",
            module_path!(),
            file!(),
            line!(),
        );

        logger.flush();

        logger
            .writer
            .lock()
            .unwrap()
            .get_mut()
            .get_ref()
            .assert_expectations(start_time, vec![
                (Level::Info, "This should get logged"),
                (Level::Warn, "And of course this"),
                (Level::Error, "This has to get logged, otherwise would be bad"),
            ]);
    }
}
