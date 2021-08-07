use mqs_common::{QueueConfig, QueueRedrivePolicy};
use std::{
    env::args,
    io::{stdin, Read},
    str::FromStr,
};

pub enum ParsedArgs {
    ShowHelp(Option<String>),
    ShowCommandHelp(Option<String>, Command),
    RunCommand(String, u16, Command),
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct OwnedPublishableMessage {
    /// Content type of the message.
    pub(crate) content_type:     String,
    /// Content encoding of the message.
    pub(crate) content_encoding: Option<String>,
    /// Encoded body of the message.
    pub(crate) message:          Vec<u8>,
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

    parse_args(arg_vec)
}

fn parse_args(args: Vec<String>) -> ParsedArgs {
    match parse_top_options(args) {
        Err(msg) => ParsedArgs::ShowHelp(msg),
        Ok((arg_vec, host, port)) => match parse_cmd(arg_vec) {
            Err(result) => result,
            Ok(cmd) => ParsedArgs::RunCommand(host, port, cmd),
        },
    }
}

fn parse_top_options(mut args: Vec<String>) -> Result<(Vec<String>, String, u16), Option<String>> {
    let mut host = "localhost".to_string();
    let mut port = 7843;
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

    Ok((args, host, port))
}

fn parse_cmd(mut args: Vec<String>) -> Result<Command, ParsedArgs> {
    match args.pop() {
        None => Err(ParsedArgs::ShowHelp(None)),
        Some(cmd) => {
            let s: &str = &cmd;
            match s {
                "queue" => parse_queue_cmd(args),
                "message" => parse_message_cmd(args),
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

fn parse_message_cmd(mut args: Vec<String>) -> Result<Command, ParsedArgs> {
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
                    parse_queue_and_message(args).map(|(queue, message)| Command::PublishMessage(queue, message))
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
fn parse_queue_and_message(mut args: Vec<String>) -> Result<(String, OwnedPublishableMessage), ParsedArgs> {
    let mut queue_name = None;
    let mut content_type = None;
    let mut content_encoding = None;
    let cmd = Command::PublishMessage(String::new(), OwnedPublishableMessage {
        content_type:     String::new(),
        content_encoding: None,
        message:          Vec::new(),
    });

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
    stdin()
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
