use mqs_common::{QueueConfig, QueueRedrivePolicy};
use std::{
    env::args,
    io::{stdin, Read},
    str::FromStr,
};
use uuid::Uuid;

#[derive(Eq, PartialEq, Debug)]
pub enum ParsedArgs {
    ShowHelp(Option<String>),
    ShowCommandHelp(Option<String>, Command),
    RunCommand(String, u16, Option<Uuid>, Command),
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum Command {
    ListQueues(Option<usize>, Option<usize>),
    CreateQueue(String, QueueConfig),
    UpdateQueue(String, QueueConfig),
    DeleteQueue(String),
    DescribeQueue(String),
    ReceiveMessage(String, Option<u16>),
    ReceiveMessages(String, u16, Option<u16>),
    PublishMessage(String, OwnedPublishableMessage),
    DeleteMessage(String),
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct OwnedPublishableMessage {
    /// Content type of the message.
    pub(crate) content_type:     String,
    /// Content encoding of the message.
    pub(crate) content_encoding: Option<String>,
    /// Encoded body of the message.
    pub(crate) message:          Vec<u8>,
}

struct TopOptions {
    remaining_args: Vec<String>,
    host:           String,
    port:           u16,
    trace_id:       Option<Uuid>,
}

#[must_use]
pub fn parse_os_args() -> ParsedArgs {
    let mut arg_vec = Vec::new();
    let mut has_first = false;

    for arg in args() {
        if has_first {
            arg_vec.push(arg);
        } else {
            has_first = true;
        }
    }

    let mut input = stdin();
    parse_args(&mut input, arg_vec)
}

fn parse_args<R: Read>(input: &mut R, args: Vec<String>) -> ParsedArgs {
    match parse_top_options(args) {
        Err(msg) => ParsedArgs::ShowHelp(msg),
        Ok(opts) => match parse_cmd(input, opts.remaining_args) {
            Err(result) => result,
            Ok(cmd) => ParsedArgs::RunCommand(opts.host, opts.port, opts.trace_id, cmd),
        },
    }
}

fn parse_top_options(mut args: Vec<String>) -> Result<TopOptions, Option<String>> {
    let mut host = "localhost".to_string();
    let mut port = 7843;
    let mut trace_id = None;
    args.reverse();

    loop {
        match args.last() {
            None => break,
            Some(s) => {
                let s: &str = s;
                match s {
                    "--host" => {
                        args.pop();
                        if let Some(new_host) = args.pop() {
                            host = new_host;
                        } else {
                            return Err(Some("Missing argument to --host".to_string()));
                        }
                    },
                    "--port" => {
                        args.pop();
                        if let Some(new_port) = args.pop() {
                            match new_port.parse() {
                                Err(err) => {
                                    return Err(Some(format!("Failed to parse {} as port: {}", new_port, err)));
                                },
                                Ok(new_port) => {
                                    port = new_port;
                                },
                            };
                        } else {
                            return Err(Some("Missing argument to --port".to_string()));
                        }
                    },
                    "--trace-id" => {
                        args.pop();
                        if let Some(new_trace_id) = args.pop() {
                            match Uuid::parse_str(&new_trace_id) {
                                Err(err) => {
                                    return Err(Some(format!("Failed to parse {} as trace id: {}", new_trace_id, err)));
                                },
                                Ok(new_trace_id) => {
                                    trace_id = Some(new_trace_id);
                                },
                            };
                        } else {
                            return Err(Some("Missing argument to --trace-id".to_string()));
                        }
                    },
                    "--help" => return Err(None),
                    _ => {
                        if s.starts_with('-') {
                            return Err(Some(format!("Unrecognized option {}", s)));
                        }

                        break;
                    },
                }
            },
        };
    }

    Ok(TopOptions {
        remaining_args: args,
        host,
        port,
        trace_id,
    })
}

fn parse_cmd<R: Read>(input: &mut R, mut args: Vec<String>) -> Result<Command, ParsedArgs> {
    match args.pop() {
        None => Err(ParsedArgs::ShowHelp(None)),
        Some(cmd) => {
            let s: &str = &cmd;
            match s {
                "queue" => parse_queue_cmd(args),
                "message" => parse_message_cmd(input, args),
                "help" => Err(ParsedArgs::ShowHelp(None)),
                _ => Err(ParsedArgs::ShowHelp(Some(format!("Unrecognized command {}", cmd)))),
            }
        },
    }
}

fn parse_queue_cmd(mut args: Vec<String>) -> Result<Command, ParsedArgs> {
    match args.pop() {
        None => Err(ParsedArgs::ShowHelp(None)),
        Some(sub_cmd) => {
            let s: &str = &sub_cmd;
            match s {
                "create" => {
                    parse_queue_name_and_config(args, Command::CreateQueue(String::new(), empty_queue_config()))
                        .map(|(queue_name, queue_config)| Command::CreateQueue(queue_name, queue_config))
                },
                "update" => {
                    parse_queue_name_and_config(args, Command::UpdateQueue(String::new(), empty_queue_config()))
                        .map(|(queue_name, queue_config)| Command::UpdateQueue(queue_name, queue_config))
                },
                "delete" => parse_queue_name(args, Command::DeleteQueue(String::new())).map(Command::DeleteQueue),
                "list" => parse_limit_offset(args).map(|(offset, limit)| Command::ListQueues(offset, limit)),
                "describe" => parse_queue_name(args, Command::DescribeQueue(String::new())).map(Command::DescribeQueue),
                "help" => Err(ParsedArgs::ShowHelp(None)),
                _ => Err(ParsedArgs::ShowHelp(Some(format!(
                    "Unrecognized queue subcommand {}",
                    sub_cmd
                )))),
            }
        },
    }
}

const fn empty_queue_config() -> QueueConfig {
    QueueConfig {
        redrive_policy:        None,
        retention_timeout:     0,
        visibility_timeout:    0,
        message_delay:         0,
        message_deduplication: false,
    }
}

const fn empty_owned_publishable_message() -> OwnedPublishableMessage {
    OwnedPublishableMessage {
        content_type:     String::new(),
        content_encoding: None,
        message:          Vec::new(),
    }
}

fn parse_message_cmd<R: Read>(input: &mut R, mut args: Vec<String>) -> Result<Command, ParsedArgs> {
    match args.pop() {
        None => Err(ParsedArgs::ShowHelp(None)),
        Some(sub_cmd) => {
            let s: &str = &sub_cmd;
            match s {
                "receive" => parse_queue_limit_and_timeout(args).map(|(queue, limit, timeout)| {
                    if limit == 1 {
                        Command::ReceiveMessage(queue, timeout)
                    } else {
                        Command::ReceiveMessages(queue, limit, timeout)
                    }
                }),
                "publish" => {
                    parse_queue_and_message(input, args).map(|(queue, message)| Command::PublishMessage(queue, message))
                },
                "delete" => parse_message_id(args).map(Command::DeleteMessage),
                "help" => Err(ParsedArgs::ShowHelp(None)),
                _ => Err(ParsedArgs::ShowHelp(Some(format!(
                    "Unrecognized message subcommand {}",
                    sub_cmd
                )))),
            }
        },
    }
}

fn parse_single_arg_string(
    args: &mut Vec<String>,
    cmd: &Command,
    error_msg: &'static str,
) -> Result<String, ParsedArgs> {
    args.pop().map_or_else(
        || Err(ParsedArgs::ShowCommandHelp(Some(error_msg.to_string()), cmd.clone())),
        Ok,
    )
}

fn parse_single_arg<T: FromStr, F: FnOnce(&str, <T as FromStr>::Err) -> String>(
    args: &mut Vec<String>,
    cmd: &Command,
    missing_error_msg: &'static str,
    mk_parse_error: F,
) -> Result<T, ParsedArgs> {
    let val = parse_single_arg_string(args, cmd, missing_error_msg)?;
    val.parse()
        .map_err(|err| ParsedArgs::ShowCommandHelp(Some(mk_parse_error(&val, err)), cmd.clone()))
}

#[allow(clippy::too_many_lines)]
fn parse_queue_name_and_config(mut args: Vec<String>, cmd: Command) -> Result<(String, QueueConfig), ParsedArgs> {
    let mut queue_name = None;
    let mut max_receives = None;
    let mut dead_letter_queue = None;
    let mut retention_timeout = None;
    let mut visibility_timeout = None;
    let mut message_delay = 0;
    let mut message_deduplication = false;

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--queue-name" => {
                queue_name = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --queue-name. You need to specify the queue to operate on.",
                )?);
            },
            "--dead-letter-queue" => {
                dead_letter_queue = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --dead-letter-queue. You need to specify the name of the dead letter queue.",
                )?);
            },
            "--max-receives" => {
                max_receives = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --max-receives. You need to specify the maximum number of receives on the queue.",
                    |val, err| format!("Failed to parse {} as maximum number of receives: {}", val, err),
                )?);
            },
            "--retention-timeout" => {
                retention_timeout = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --retention-timeout. You need to specify the number of seconds a message will be stored on the server.",
                    |val, err| format!("Failed to parse {} as retention timeout: {}", val, err),
                )?);
            },
            "--visibility-timeout" => {
                visibility_timeout = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --visibility-timeout. You need to specify the number of seconds a message will be hidden after it was received.",
                    |val, err| format!("Failed to parse {} as visibility timeout: {}", val, err),
                )?);
            },
            "--message-delay" => {
                message_delay = parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --message-delay. You need to specify the number of seconds a message will be hidden after it was first published.",
                    |val, err| format!("Failed to parse {} as maximum number of receives: {}", val, err),
                )?;
            },
            "--message-deduplication" => {
                message_deduplication = parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --message-deduplication. You need to specify whether duplicate messages should be dropped from the queue.",
                    |val, err| format!("Failed to parse {} as message deduplication: {}", val, err),
                )?;
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    let queue_name = if let Some(queue_name) = queue_name {
        queue_name
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.".to_string()),
            cmd,
        ));
    };

    let redrive_policy = if let Some(max_receives) = max_receives {
        if let Some(dead_letter_queue) = dead_letter_queue {
            Some(QueueRedrivePolicy {
                max_receives,
                dead_letter_queue,
            })
        } else {
            return Err(ParsedArgs::ShowCommandHelp(
                Some("You have to specify the dead letter queue if you specify a maximum number of receives. You can use --dead-letter-queue [QUEUE] to specify it.".to_string()),
                cmd,
            ));
        }
    } else if dead_letter_queue.is_some() {
        return Err(ParsedArgs::ShowCommandHelp(
            Some("You have to specify the maximum number of receives if you specify a dead letter queue. You can use --max-receives [NUMBER] to specify it.".to_string()),
            cmd,
        ));
    } else {
        None
    };

    let retention_timeout = if let Some(retention_timeout) = retention_timeout {
        retention_timeout
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some(
                "You have to specify the retention timeout. You can use --retention-timeout [SECONDS] to specify it."
                    .to_string(),
            ),
            cmd,
        ));
    };

    let visibility_timeout = if let Some(visibility_timeout) = visibility_timeout {
        visibility_timeout
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some(
                "You have to specify the visibility timeout. You can use --visibility-timeout [SECONDS] to specify it."
                    .to_string(),
            ),
            cmd,
        ));
    };

    Ok((queue_name, QueueConfig {
        redrive_policy,
        retention_timeout,
        visibility_timeout,
        message_delay,
        message_deduplication,
    }))
}

fn parse_limit_offset(mut args: Vec<String>) -> Result<(Option<usize>, Option<usize>), ParsedArgs> {
    let mut offset = None;
    let mut limit = None;
    let cmd = Command::ListQueues(None, None);

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--offset" => {
                offset = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --offset. You need to specify the number of queues to skip.",
                    |val, err| format!("Failed to parse {} as number of queues to skip: {}", val, err),
                )?);
            },
            "--limit" => {
                limit = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --limit. You need to specify the maximum number of queues to list.",
                    |val, err| format!("Failed to parse {} as maximum number of queues to list: {}", val, err),
                )?);
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    Ok((offset, limit))
}

fn parse_queue_name(mut args: Vec<String>, cmd: Command) -> Result<String, ParsedArgs> {
    let mut queue_name = None;

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--queue-name" => {
                queue_name = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --queue-name. You need to specify the queue to operate on.",
                )?);
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    let queue_name = if let Some(queue_name) = queue_name {
        queue_name
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.".to_string()),
            cmd,
        ));
    };

    Ok(queue_name)
}

fn parse_queue_limit_and_timeout(mut args: Vec<String>) -> Result<(String, u16, Option<u16>), ParsedArgs> {
    let mut queue_name = None;
    let mut limit = 1;
    let mut timeout = None;
    let cmd = Command::ReceiveMessages(String::new(), 0, None);

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--queue-name" => {
                queue_name = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --queue-name. You need to specify the queue to operate on.",
                )?);
            },
            "--limit" => {
                limit = parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --limit. You need to specify the maximum number of messages to retrieve.",
                    |val, err| {
                        format!(
                            "Failed to parse {} as maximum number of messages to retrieve: {}",
                            val, err
                        )
                    },
                )?;
            },
            "--timeout" => {
                timeout = Some(parse_single_arg(
                    &mut args,
                    &cmd,
                    "Missing argument to --timeout. You need to specify the maximum number of seconds to wait.",
                    |val, err| format!("Failed to parse {} as maximum number of seconds to wait: {}", val, err),
                )?);
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    let queue_name = if let Some(queue_name) = queue_name {
        queue_name
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.".to_string()),
            cmd,
        ));
    };

    Ok((queue_name, limit, timeout))
}

fn parse_queue_and_message<R: Read>(
    input: &mut R,
    mut args: Vec<String>,
) -> Result<(String, OwnedPublishableMessage), ParsedArgs> {
    let mut queue_name = None;
    let mut content_type = None;
    let mut content_encoding = None;
    let cmd = Command::PublishMessage(String::new(), empty_owned_publishable_message());

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--queue-name" => {
                queue_name = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --queue-name. You need to specify the queue to operate on.",
                )?);
            },
            "--content-type" => {
                content_type = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --content-type. You need to specify the content-type of the message.",
                )?);
            },
            "--content-encoding" => {
                content_encoding = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --content-encoding. You need to specify the content-encoding of the message.",
                )?);
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    let queue_name = if let Some(queue_name) = queue_name {
        queue_name
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.".to_string()),
            cmd,
        ));
    };

    let content_type = if let Some(content_type) = content_type {
        content_type
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some(
                "You have to specify the content type. You can use --content-type [CONTENT TYPE] to specify it."
                    .to_string(),
            ),
            cmd,
        ));
    };

    let mut message = Vec::new();
    input
        .read_to_end(&mut message)
        .map_err(|err| ParsedArgs::ShowCommandHelp(Some(format!("Failed to read message from stdin: {}", err)), cmd))?;

    Ok((queue_name, OwnedPublishableMessage {
        content_type,
        content_encoding,
        message,
    }))
}

fn parse_message_id(mut args: Vec<String>) -> Result<String, ParsedArgs> {
    let mut message_id = None;
    let cmd = Command::DeleteMessage(String::new());

    while let Some(arg) = args.pop() {
        let s: &str = &arg;
        match s {
            "--message-id" => {
                message_id = Some(parse_single_arg_string(
                    &mut args,
                    &cmd,
                    "Missing argument to --message-id. You need to specify the id of the message.",
                )?);
            },
            "help" | "--help" => {
                return Err(ParsedArgs::ShowCommandHelp(None, cmd));
            },
            _ => {
                return Err(ParsedArgs::ShowCommandHelp(
                    Some(format!("Unrecognized argument {}", arg)),
                    cmd,
                ));
            },
        }
    }

    let message_id = if let Some(message_id) = message_id {
        message_id
    } else {
        return Err(ParsedArgs::ShowCommandHelp(
            Some(
                "You have to specify the message id. You can use --message-id [MESSAGE ID] to specify it.".to_string(),
            ),
            cmd,
        ));
    };

    Ok(message_id)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::args::{Command::*, ParsedArgs::*};

    fn mk_show_help(s: &str) -> ParsedArgs {
        ShowHelp(Some(s.to_string()))
    }

    fn mk_show_command_help(cmd: &Command) -> ParsedArgs {
        ShowCommandHelp(None, cmd.clone())
    }

    fn mk_show_command_help_with_message(s: &str, cmd: &Command) -> ParsedArgs {
        ShowCommandHelp(Some(s.to_string()), cmd.clone())
    }

    fn mk_run_command(cmd: Command) -> ParsedArgs {
        RunCommand("localhost".to_string(), 7843, None, cmd)
    }

    struct TestCase {
        args:     Vec<&'static str>,
        input:    &'static str,
        expected: ParsedArgs,
    }

    fn no_input(args: Vec<&'static str>, expected: ParsedArgs) -> TestCase {
        with_input(args, "", expected)
    }

    fn with_input(args: Vec<&'static str>, input: &'static str, expected: ParsedArgs) -> TestCase {
        TestCase { args, input, expected }
    }

    #[test]
    fn parse_args() {
        let create_queue = CreateQueue(String::new(), empty_queue_config());
        let update_queue = UpdateQueue(String::new(), empty_queue_config());
        let delete_queue = DeleteQueue(String::new());
        let list_queues = ListQueues(None, None);
        let describe_queue = DescribeQueue(String::new());
        let receive_messages = ReceiveMessages(String::new(), 0, None);
        let publish_message = PublishMessage(String::new(), empty_owned_publishable_message());
        let delete_message = DeleteMessage(String::new());

        let test_cases = [
            no_input(vec![], ShowHelp(None)),
            no_input(vec!["help"], ShowHelp(None)),
            no_input(vec!["invalid"], mk_show_help("Unrecognized command invalid")),
            no_input(vec!["--help"], ShowHelp(None)),
            no_input(vec!["--invalid"], mk_show_help("Unrecognized option --invalid")),
            no_input(vec!["--host"], mk_show_help("Missing argument to --host")),
            no_input(vec!["--port"], mk_show_help("Missing argument to --port")),
            no_input(vec!["--trace-id"], mk_show_help("Missing argument to --trace-id")),
            no_input(vec!["--host", "hostname"], ShowHelp(None)),
            no_input(vec!["--host", "hostname", "--help"], ShowHelp(None)),
            no_input(vec!["--host", "hostname", "--port", "1234", "--help"], ShowHelp(None)),
            no_input(vec!["--port", "not a port"], mk_show_help("Failed to parse not a port as port: invalid digit found in string")),
            no_input(vec!["--port", "1234"], ShowHelp(None)),
            no_input(vec!["--port", "1234", "--help"], ShowHelp(None)),
            no_input(vec!["--trace-id", "4aa662d5-b5c9-4f1c-b4ce-09e7ca6c57a5"], ShowHelp(None)),
            no_input(vec!["--trace-id", "not a uuid"], mk_show_help("Failed to parse not a uuid as trace id: invalid length: expected one of [36, 32], found 10")),
            no_input(vec!["--trace-id", "4aa662d5-b5c9-4f1c-b4ce-09e7ca6c57a5", "--help"], ShowHelp(None)),
            no_input(vec!["queue", "help"], ShowHelp(None)),
            no_input(vec!["message", "help"], ShowHelp(None)),
            no_input(vec!["queue", "create", "help"], mk_show_command_help(&create_queue)),
            no_input(vec!["queue", "update", "help"], mk_show_command_help(&update_queue)),
            no_input(vec!["queue", "delete", "help"], mk_show_command_help(&delete_queue)),
            no_input(vec!["queue", "list", "help"], mk_show_command_help(&list_queues)),
            no_input(vec!["queue", "describe", "help"], mk_show_command_help(&describe_queue)),
            no_input(vec!["message", "receive", "help"], mk_show_command_help(&receive_messages)),
            no_input(vec!["message", "publish", "help"], mk_show_command_help(&publish_message)),
            no_input(vec!["message", "delete", "help"], mk_show_command_help(&delete_message)),
            no_input(vec!["queue", "create"], mk_show_command_help_with_message("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue"], mk_show_command_help_with_message("You have to specify the retention timeout. You can use --retention-timeout [SECONDS] to specify it.", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "300"], mk_show_command_help_with_message("You have to specify the visibility timeout. You can use --visibility-timeout [SECONDS] to specify it.", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "300", "--visibility-timeout", "30"], mk_run_command(CreateQueue("test-queue".to_string(), QueueConfig {
                redrive_policy: None,
                retention_timeout: 300,
                visibility_timeout: 30,
                message_delay: 0,
                message_deduplication: false,
            }))),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "300", "--visibility-timeout", "30", "--dead-letter-queue", "dead-queue", "--max-receives", "10", "--message-delay", "15", "--message-deduplication", "true"], mk_run_command(CreateQueue("test-queue".to_string(), QueueConfig {
                redrive_policy: Some(QueueRedrivePolicy {
                    dead_letter_queue: "dead-queue".to_string(),
                    max_receives: 10,
                }),
                retention_timeout: 300,
                visibility_timeout: 30,
                message_delay: 15,
                message_deduplication: true,
            }))),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "300", "--visibility-timeout", "30", "--dead-letter-queue", "dead-queue"], mk_show_command_help_with_message("You have to specify the maximum number of receives if you specify a dead letter queue. You can use --max-receives [NUMBER] to specify it.", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "300", "--visibility-timeout", "30", "--max-receives", "10"], mk_show_command_help_with_message("You have to specify the dead letter queue if you specify a maximum number of receives. You can use --dead-letter-queue [QUEUE] to specify it.", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--max-receives", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as maximum number of receives: invalid digit found in string", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--retention-timeout", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as retention timeout: invalid digit found in string", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--visibility-timeout", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as visibility timeout: invalid digit found in string", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--message-delay", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as maximum number of receives: invalid digit found in string", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--message-deduplication", "not a bool"], mk_show_command_help_with_message("Failed to parse not a bool as message deduplication: provided string was not `true` or `false`", &create_queue)),
            no_input(vec!["queue", "create", "--queue-name", "test-queue", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &create_queue)),
            no_input(vec!["queue", "update", "--queue-name", "test-queue", "--retention-timeout", "300", "--visibility-timeout", "30"], mk_run_command(UpdateQueue("test-queue".to_string(), QueueConfig {
                redrive_policy: None,
                retention_timeout: 300,
                visibility_timeout: 30,
                message_delay: 0,
                message_deduplication: false,
            }))),
            no_input(vec!["queue", "invalid"], mk_show_help("Unrecognized queue subcommand invalid")),
            no_input(vec!["queue", "list"], mk_run_command(ListQueues(None, None))),
            no_input(vec!["queue", "list", "--offset", "20"], mk_run_command(ListQueues(Some(20), None))),
            no_input(vec!["queue", "list", "--offset", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as number of queues to skip: invalid digit found in string", &list_queues)),
            no_input(vec!["queue", "list", "--limit", "10"], mk_run_command(ListQueues(None, Some(10)))),
            no_input(vec!["queue", "list", "--limit", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as maximum number of queues to list: invalid digit found in string", &list_queues)),
            no_input(vec!["queue", "list", "--offset", "20", "--limit", "10"], mk_run_command(ListQueues(Some(20), Some(10)))),
            no_input(vec!["queue", "list", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &list_queues)),
            no_input(vec!["queue", "delete"], mk_show_command_help_with_message("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.", &delete_queue)),
            no_input(vec!["queue", "delete", "--queue-name"], mk_show_command_help_with_message("Missing argument to --queue-name. You need to specify the queue to operate on.", &delete_queue)),
            no_input(vec!["queue", "delete", "--queue-name", "delete-this"], mk_run_command(DeleteQueue("delete-this".to_string()))),
            no_input(vec!["queue", "delete", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &delete_queue)),
            no_input(vec!["queue", "describe"], mk_show_command_help_with_message("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.", &describe_queue)),
            no_input(vec!["queue", "describe", "--queue-name"], mk_show_command_help_with_message("Missing argument to --queue-name. You need to specify the queue to operate on.", &describe_queue)),
            no_input(vec!["queue", "describe", "--queue-name", "describe-this"], mk_run_command(DescribeQueue("describe-this".to_string()))),
            no_input(vec!["queue", "describe", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &describe_queue)),
            no_input(vec!["message", "invalid"], mk_show_help("Unrecognized message subcommand invalid")),
            no_input(vec!["message", "receive"], mk_show_command_help_with_message("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name"], mk_show_command_help_with_message("Missing argument to --queue-name. You need to specify the queue to operate on.", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name", "test-queue"], mk_run_command(ReceiveMessage("test-queue".to_string(), None))),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--limit"], mk_show_command_help_with_message("Missing argument to --limit. You need to specify the maximum number of messages to retrieve.", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--limit", "5"], mk_run_command(ReceiveMessages("test-queue".to_string(), 5, None))),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--limit", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as maximum number of messages to retrieve: invalid digit found in string", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--timeout"], mk_show_command_help_with_message("Missing argument to --timeout. You need to specify the maximum number of seconds to wait.", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--timeout", "10"], mk_run_command(ReceiveMessage("test-queue".to_string(), Some(10)))),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--timeout", "not a number"], mk_show_command_help_with_message("Failed to parse not a number as maximum number of seconds to wait: invalid digit found in string", &receive_messages)),
            no_input(vec!["message", "receive", "--queue-name", "test-queue", "--limit", "5", "--timeout", "10"], mk_run_command(ReceiveMessages("test-queue".to_string(), 5, Some(10)))),
            no_input(vec!["message", "receive", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &receive_messages)),
            no_input(vec!["message", "publish"], mk_show_command_help_with_message("You have to specify a queue. You can use --queue-name [QUEUE] to specify one.", &publish_message)),
            no_input(vec!["message", "publish", "--queue-name"], mk_show_command_help_with_message("Missing argument to --queue-name. You need to specify the queue to operate on.", &publish_message)),
            no_input(vec!["message", "publish", "--queue-name", "test-queue"], mk_show_command_help_with_message("You have to specify the content type. You can use --content-type [CONTENT TYPE] to specify it.", &publish_message)),
            no_input(vec!["message", "publish", "--queue-name", "test-queue", "--content-type"], mk_show_command_help_with_message("Missing argument to --content-type. You need to specify the content-type of the message.", &publish_message)),
            with_input(vec!["message", "publish", "--queue-name", "test-queue", "--content-type", "text/plain"], "abc", mk_run_command(PublishMessage("test-queue".to_string(), OwnedPublishableMessage {
                content_type: "text/plain".to_string(),
                content_encoding: None,
                message: "abc".as_bytes().to_vec(),
            }))),
            no_input(vec!["message", "publish", "--queue-name", "test-queue", "--content-type", "text/plain", "--content-encoding"], mk_show_command_help_with_message("Missing argument to --content-encoding. You need to specify the content-encoding of the message.", &publish_message)),
            with_input(vec!["message", "publish", "--queue-name", "test-queue", "--content-type", "text/plain", "--content-encoding", "identity"], "abc", mk_run_command(PublishMessage("test-queue".to_string(), OwnedPublishableMessage {
                content_type: "text/plain".to_string(),
                content_encoding: Some("identity".to_string()),
                message: "abc".as_bytes().to_vec(),
            }))),
            no_input(vec!["message", "publish", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &publish_message)),
            no_input(vec!["message", "delete"], mk_show_command_help_with_message("You have to specify the message id. You can use --message-id [MESSAGE ID] to specify it.", &delete_message)),
            no_input(vec!["message", "delete", "--message-id"], mk_show_command_help_with_message("Missing argument to --message-id. You need to specify the id of the message.", &delete_message)),
            no_input(vec!["message", "delete", "--message-id", "test-message"], mk_run_command(DeleteMessage("test-message".to_string()))),
            no_input(vec!["message", "delete", "--invalid"], mk_show_command_help_with_message("Unrecognized argument --invalid", &delete_message)),
        ];

        for test_case in test_cases {
            let args = {
                let mut v = Vec::with_capacity(test_case.args.len());
                for arg in test_case.args {
                    v.push(arg.to_string());
                }
                v
            };

            let parsed = super::parse_args(&mut test_case.input.as_bytes(), args.clone());
            assert_eq!(
                parsed, test_case.expected,
                "Parsing '{:?}' should yield {:?} but got {:?}",
                args, test_case.expected, parsed
            );
        }
    }
}
