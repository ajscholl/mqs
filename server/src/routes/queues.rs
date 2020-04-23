use diesel::QueryResult;
use hyper::Body;
use mqs_common::{status::Status, QueueConfig, QueuesResponse};
use std::collections::HashMap;

use crate::{
    models::queue::{QueueInput, QueueRepository},
    routes::MqsResponse,
};

pub fn new_queue<R: QueueRepository>(
    repo: R,
    queue_name: &str,
    params: Result<QueueConfig, serde_json::Error>,
) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(err_message)
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

pub fn update_queue<R: QueueRepository>(
    repo: R,
    queue_name: &str,
    params: Result<QueueConfig, serde_json::Error>,
) -> MqsResponse {
    match params {
        Err(err) => {
            let err_message = format!("{:?}", err);
            error!("Failed to parse queue params: {}", &err_message);
            MqsResponse::error_owned(err_message)
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

pub fn delete_queue<R: QueueRepository>(repo: R, queue_name: &str) -> MqsResponse {
    info!("Deleting queue {}", queue_name);
    let deleted = repo.delete_queue_by_name(queue_name);
    match deleted {
        Ok(Some(queue)) => {
            info!("Deleted queue {}", queue_name);
            MqsResponse::json(&queue.into_config_output())
        },
        Ok(None) => {
            info!("Queue {} was not found", queue_name);
            MqsResponse::status(Status::NotFound)
        },
        Err(err) => {
            error!("Failed to delete queue {}: {}", queue_name, err);
            MqsResponse::status(Status::InternalServerError)
        },
    }
}

#[derive(Debug)]
pub struct QueuesRange {
    offset: Option<i64>,
    limit:  Option<i64>,
}

impl QueuesRange {
    pub fn from_hyper(req: hyper::Request<Body>) -> Result<QueuesRange, String> {
        let query = req.uri().query().unwrap_or("");
        let mut query_params = HashMap::new();
        for param in query.split("&").into_iter() {
            if param.is_empty() {
                continue;
            }

            // TODO: add proper query param parsing and url param decoding
            let mut i = param.splitn(2, "=").into_iter();
            let name = i.next().unwrap_or("");
            let value = i.next().unwrap_or("");
            debug_assert!(i.next().is_none());
            query_params.insert(name, value);
        }
        let offset = query_params
            .get("offset")
            .map_or_else(|| Ok(None), |s| s.parse().map_or_else(|e| Err(e), |v| Ok(Some(v))));
        let limit = query_params
            .get("limit")
            .map_or_else(|| Ok(None), |s| s.parse().map_or_else(|e| Err(e), |v| Ok(Some(v))));

        match (offset, limit) {
            (Err(err), _) => Err(format!("invalid value for number field offset: {}", err)),
            (_, Err(err)) => Err(format!("invalid value for number field limit: {}", err)),
            (Ok(offset), Ok(limit)) => Ok(QueuesRange { offset, limit }),
        }
    }
}

fn list_queues_and_count<R: QueueRepository>(repo: R, range: &QueuesRange) -> QueryResult<QueuesResponse> {
    let queues = repo.list_queues(range.offset, range.limit)?;
    let total = repo.count_queues()?;
    Ok(QueuesResponse {
        queues: queues.into_iter().map(|queue| queue.into_config_output()).collect(),
        total,
    })
}

pub fn list_queues<R: QueueRepository>(repo: R, range: Result<QueuesRange, String>) -> MqsResponse {
    match range {
        Err(err) => MqsResponse::error_owned(err),
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

pub fn describe_queue<R: QueueRepository>(repo: R, queue_name: &str) -> MqsResponse {
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
