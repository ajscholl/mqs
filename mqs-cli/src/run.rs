use crate::args::Command;
use mqs_client::{ClientError, MessageResponse, PublishableMessage, Service};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize, Debug)]
struct ErrorStruct {
    err: String,
}

#[derive(Serialize, Debug)]
struct PublishedStruct {
    published: bool,
}

#[derive(Serialize, Debug)]
struct SuccessStruct {
    success: bool,
}

#[derive(Serialize, Debug)]
struct MessageStruct {
    pub message_id:       String,
    pub content_type:     String,
    pub content_encoding: Option<String>,
    pub message_receives: i32,
    pub published_at:     String,
    pub visible_at:       String,
    pub trace_id:         Option<String>,
    pub content:          String,
}

fn print_json<T: ?Sized + Serialize>(json: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(json).expect("Failed to format value as JSON")
    );
}

fn print_opt_queue_config<T: Sized + Serialize, F: FnOnce() -> String>(response: Option<T>, mk_error: F) -> i32 {
    response.map_or_else(
        || {
            print_json(&ErrorStruct { err: mk_error() });

            2
        },
        |response| {
            print_json(&response);

            0
        },
    )
}

fn print_messages(messages: Vec<MessageResponse>) {
    for message in messages {
        print_json(&MessageStruct {
            message_id:       message.message_id,
            content_type:     message.content_type,
            content_encoding: message.content_encoding,
            message_receives: message.message_receives,
            published_at:     message.published_at.to_rfc3339(),
            visible_at:       message.visible_at.to_rfc3339(),
            trace_id:         message.trace_id.map(|trace_id| trace_id.to_string()),
            content:          base64::encode(message.content),
        });
    }
}

pub async fn run_command(host: &str, port: u16, trace_id: Option<Uuid>, cmd: Command) -> i32 {
    match run_command_for_result(host, port, trace_id, cmd).await {
        Ok(code) => code,
        Err(err) => {
            print_json(&ErrorStruct {
                err: format!("{}", err),
            });

            1
        },
    }
}

async fn run_command_for_result(
    host: &str,
    port: u16,
    trace_id: Option<Uuid>,
    cmd: Command,
) -> Result<i32, ClientError> {
    let s = Service::new(&format_host(host, port));

    match cmd {
        Command::ListQueues(offset, limit) => {
            let queues = s.get_queues(trace_id, offset, limit).await?;
            print_json(&queues);
        },
        Command::CreateQueue(queue_name, config) => {
            let response = s.create_queue(&queue_name, trace_id, &config).await?;
            return Ok(print_opt_queue_config(response, || {
                format!("queue {} already exists", queue_name)
            }));
        },
        Command::UpdateQueue(queue_name, config) => {
            let response = s.update_queue(&queue_name, trace_id, &config).await?;
            return Ok(print_opt_queue_config(response, || {
                format!("queue {} does not exist", queue_name)
            }));
        },
        Command::DeleteQueue(queue_name) => {
            let response = s.delete_queue(&queue_name, trace_id).await?;
            return Ok(print_opt_queue_config(response, || {
                format!("queue {} does not exist", queue_name)
            }));
        },
        Command::DescribeQueue(queue_name) => {
            let response = s.describe_queue(&queue_name, trace_id).await?;
            return Ok(print_opt_queue_config(response, || {
                format!("queue {} does not exist", queue_name)
            }));
        },
        Command::ReceiveMessage(queue_name, timeout) => {
            let message = s.get_message(&queue_name, timeout).await?;
            print_messages(message.map_or_else(Vec::new, |message| vec![message]));
        },
        Command::ReceiveMessages(queue_name, limit, timeout) => {
            let messages = s.get_messages(&queue_name, limit, timeout).await?;
            print_messages(messages);
        },
        Command::PublishMessage(queue_name, message) => {
            let published = s
                .publish_message(&queue_name, PublishableMessage {
                    content_type: &message.content_type,
                    content_encoding: message.content_encoding.as_deref(),
                    trace_id,
                    message: message.message,
                })
                .await?;
            print_json(&PublishedStruct { published });
        },
        Command::DeleteMessage(message_id) => {
            let deleted = s.delete_message(trace_id, &message_id).await?;
            if !deleted {
                print_json(&ErrorStruct {
                    err: format!("message {} did not exist", &message_id),
                });

                return Ok(2);
            }

            print_json(&SuccessStruct { success: true });
        },
    }

    Ok(0)
}

// noinspection HttpUrlsUsage
fn format_host(host: &str, port: u16) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        format!("{}:{}", host, port)
    } else {
        format!("http://{}:{}", host, port)
    }
}
