use crate::args::Command;
use std::env::current_exe;

fn get_program_name() -> String {
    current_exe()
        .ok()
        .and_then(|path_buf| {
            let path = path_buf.as_path();
            if path.is_file() {
                path.file_name().and_then(|name| name.to_str().map(ToString::to_string))
            } else {
                None
            }
        })
        .unwrap_or_else(|| "mqs-cli".to_string())
}

pub fn show_help(error: Option<String>) {
    println!("Mini queue service");
    println!();
    println!("USAGE:");
    println!("    {} [OPTIONS] [SUBCOMMAND]", get_program_name());
    println!();
    println!("OPTIONS:");
    println!("    --host <HOST>            Specify the server host (default: localhost)");
    println!("    --port <PORT>            Specify the server port (default: 7843)");
    println!("    --trace-id <UUID>        Set a trace id for the request send to the server");
    println!("    --help                   Prints help information");
    println!();
    println!("SUBCOMMANDS:");
    println!("    queue create             Create a new queue");
    println!("    queue update             Update an existing queue");
    println!("    queue delete             Delete a queue");
    println!("    queue list               List queues");
    println!("    queue describe           Get information about a queue");
    println!("    message receive          Receive one or more messages from a queue");
    println!("    message publish          Publish a message to a queue");
    println!("    message delete           Delete a message from a queue");
    println!();
    println!(
        "See '{} command help' for more information on a specific command.",
        get_program_name()
    );

    if let Some(error) = error {
        println!();
        println!("{}", error);
    }
}

pub fn show_subcommand_help(error: Option<String>, cmd: &Command) {
    let (flags, subcommand, subcommand_description) = subcommand_help(cmd);

    let mut flags_string = String::new();
    let mut max_flag_length = 0;
    for (flag, _, required) in &flags {
        if !flags_string.is_empty() {
            flags_string.push(' ');
        }
        if !*required {
            flags_string.push('[');
        }
        flags_string.push_str(flag);
        if !*required {
            flags_string.push(']');
        }

        max_flag_length = max_flag_length.max(flag.len());
    }

    println!("Mini queue service");
    println!();
    println!("USAGE:");
    println!("    {} [OPTIONS] {} {}", get_program_name(), subcommand, flags_string);
    println!();
    println!("OPTIONS:");
    for (flag, description, _) in flags {
        let ws_len = 4 + max_flag_length - flag.len();
        let mut ws = String::with_capacity(ws_len);
        while ws.len() < ws.capacity() {
            ws.push(' ');
        }
        println!("    {}{}{}", flag, ws, description);
    }
    println!();
    println!("{}", subcommand_description);

    if let Some(error) = error {
        println!();
        println!("{}", error);
    }
}

fn subcommand_help(cmd: &Command) -> (Vec<(&'static str, &'static str, bool)>, &'static str, &'static str) {
    match cmd {
        Command::CreateQueue(_, _) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to create", true),
                ("--dead-letter-queue <QUEUE>", "The name of the dead letter queue", false),
                ("--max-receives <NUMBER>", "The maximum number of receives for a message before it is send to the dead letter queue", false),
                ("--retention-timeout <SECONDS>", "The amount of seconds before a message is deleted", true),
                ("--visibility-timeout <SECONDS>", "The amount of seconds a message is invisible after it has been received", true),
                ("--message-delay <SECONDS>", "The amount of seconds before a message is visible for the first time", false),
                ("--message-deduplication <true|false>", "Whether to drop duplicate messages", false),
            ];

            #[rustfmt::skip]
            (flags, "queue create", "Creates a new queue if it does not already exist.")
        },
        Command::UpdateQueue(_, _) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to update", true),
                ("--dead-letter-queue <QUEUE>", "The name of the dead letter queue", false),
                ("--max-receives <NUMBER>", "The maximum number of receives for a message before it is send to the dead letter queue", false),
                ("--retention-timeout <SECONDS>", "The amount of seconds before a message is deleted", true),
                ("--visibility-timeout <SECONDS>", "The amount of seconds a message is invisible after it has been received", true),
                ("--message-delay <SECONDS>", "The amount of seconds before a message is visible for the first time", false),
                ("--message-deduplication <true|false>", "Whether to drop duplicate messages", false),
            ];

            #[rustfmt::skip]
            (flags, "queue update", "Edits the configuration of a queue.")
        },
        Command::DeleteQueue(_) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to delete", true),
            ];

            #[rustfmt::skip]
            (flags, "queue delete", "Deletes a queue and all messages stored in it.")
        },
        Command::ListQueues(_, _) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--offset <NUMBER>", "The number of queues to skip", false),
                ("--limit <NUMBER>", "The maximum number of queues to return", false),
            ];

            #[rustfmt::skip]
            (flags, "queue list", "Lists existing queues.")
        },
        Command::DescribeQueue(_) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to describe", true),
            ];

            #[rustfmt::skip]
            (flags, "queue describe", "Get information about a single queue.")
        },
        Command::ReceiveMessage(_, _) | Command::ReceiveMessages(_, _, _) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to receive messages from", true),
                ("--limit <NUMBER>", "The maximum number of messages to receive", false),
                ("--timeout <SECONDS>", "The amount of seconds to wait for messages", false),
            ];

            #[rustfmt::skip]
            (flags, "message receive", "Receive messages from a queue.")
        },
        Command::PublishMessage(_, _) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--queue-name <QUEUE>", "The name of the queue to send the message to", true),
                ("--content-type <STRING>", "The content type of the message", true),
                ("--content-encoding <STRING>", "The content encoding of the message", false),
            ];

            #[rustfmt::skip]
            (flags, "message publish", "Publish a message to a queue. The message body is read from standard input.")
        },
        Command::DeleteMessage(_) => {
            #[rustfmt::skip]
            let flags = vec![
                ("--message-id <MESSAGE ID>", "The id of the message to delete", true),
            ];

            #[rustfmt::skip]
            (flags, "message delete", "Delete a message from a queue.")
        },
    }
}
