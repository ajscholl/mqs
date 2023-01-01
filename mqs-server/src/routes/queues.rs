use diesel::QueryResult;
use hyper::{Body, Request};
use mqs_common::{QueueConfig, QueuesResponse, Status};
use std::convert::TryFrom;

use crate::{
    models::queue::{Queue, QueueInput, QueueRepository},
    routes::MqsResponse,
};

pub fn new<R: QueueRepository>(
    repo: &mut R,
    queue_name: &str,
    params: Result<QueueConfig, serde_json::Error>,
) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(&err_message)
        },
        Ok(config) => {
            info!("Creating new queue {}", queue_name);
            let created = repo.insert_queue(&QueueInput::new(&config, queue_name));

            match created {
                Ok(Some(queue)) => {
                    info!("Created new queue {}", queue_name);
                    MqsResponse::status_json(Status::Created, &queue.into_config_output())
                },
                Ok(None) => {
                    info!("Queue {} did already exist", queue_name);
                    MqsResponse::status(Status::Conflict)
                },
                Err(err) => {
                    error!("Failed to create new queue {}, {:?}: {}", queue_name, config, err);
                    MqsResponse::status(Status::InternalServerError)
                },
            }
        },
    }
}

pub fn update<R: QueueRepository>(
    repo: &mut R,
    queue_name: &str,
    params: Result<QueueConfig, serde_json::Error>,
) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(&err_message)
        },
        Ok(config) => {
            info!("Updating queue {}", queue_name);
            let result = repo.update_queue(&QueueInput::new(&config, queue_name));

            match result {
                Ok(Some(queue)) => {
                    info!("Updated queue {}", queue_name);
                    MqsResponse::json(&queue.into_config_output())
                },
                Ok(None) => {
                    info!("Queue {} did not exist", queue_name);
                    MqsResponse::status(Status::NotFound)
                },
                Err(err) => {
                    error!("Failed to update queue {}, {:?}: {}", queue_name, config, err);
                    MqsResponse::status(Status::InternalServerError)
                },
            }
        },
    }
}

pub fn delete<R: QueueRepository>(repo: &mut R, queue_name: &str) -> MqsResponse {
    info!("Deleting queue {}", queue_name);
    let deleted = repo.delete_queue_by_name(queue_name);
    match deleted {
        Ok(Some(queue)) => {
            info!("Deleted queue {}", queue_name);
            MqsResponse::json(&queue.into_config_output())
        },
        Ok(None) => {
            info!("Queue {} wasp not found", queue_name);
            MqsResponse::status(Status::NotFound)
        },
        Err(err) => {
            error!("Failed to delete queue {}: {}", queue_name, err);
            MqsResponse::status(Status::InternalServerError)
        },
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Range {
    offset: Option<i64>,
    limit:  Option<i64>,
}

impl TryFrom<&Request<Body>> for Range {
    type Error = String;

    fn try_from(req: &Request<Body>) -> Result<Self, Self::Error> {
        let query = req.uri().query().unwrap_or("");
        let mut offset = Ok(None);
        let mut limit = Ok(None);
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key.as_ref() == "offset" {
                offset = value.parse().map_or_else(Err, |v| Ok(Some(v)));
            } else if key.as_ref() == "limit" {
                limit = value.parse().map_or_else(Err, |v| Ok(Some(v)));
            }
        }

        match (offset, limit) {
            (Err(err), _) => Err(format!("invalid value for number field offset: {}", err)),
            (_, Err(err)) => Err(format!("invalid value for number field limit: {}", err)),
            (Ok(offset), Ok(limit)) => Ok(Self { offset, limit }),
        }
    }
}

fn list_queues_and_count<R: QueueRepository>(repo: &mut R, range: &Range) -> QueryResult<QueuesResponse> {
    let queues = repo.list_queues(range.offset, range.limit)?;
    let total = repo.count_queues()?;
    Ok(QueuesResponse {
        queues: queues.into_iter().map(Queue::into_config_output).collect(),
        total,
    })
}

pub fn list<R: QueueRepository>(repo: &mut R, range: Result<Range, String>) -> MqsResponse {
    match range {
        Err(err) => MqsResponse::error_owned(&err),
        Ok(range) => match list_queues_and_count(repo, &range) {
            Ok(response) => MqsResponse::json(&response),
            Err(err) => {
                error!(
                    "Failed to read range of queues {:?}-{:?}: {}",
                    range.offset, range.limit, err
                );

                MqsResponse::status(Status::InternalServerError)
            },
        },
    }
}

pub fn describe<R: QueueRepository>(repo: &mut R, queue_name: &str) -> MqsResponse {
    match repo.describe_queue(queue_name) {
        Err(err) => {
            error!("Failed to describe queue {}: {}", queue_name, err);
            MqsResponse::status(Status::InternalServerError)
        },
        Ok(None) => MqsResponse::status(Status::NotFound),
        Ok(Some(description)) => MqsResponse::json(&description.queue.into_config_output().into_description(
            description.messages,
            description.visible_messages,
            description.oldest_message_age,
        )),
    }
}
