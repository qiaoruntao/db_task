use std::sync::Arc;

use async_trait::async_trait;
use mongodb::error::{ErrorKind, WriteFailure};
use tracing::{debug, error};

use crate::{TaskInfo, TaskOptions};
use crate::app::common::TaskAppCommon;
use crate::task::task_state::TaskState;
use crate::task::TaskRequest;

#[async_trait]
pub trait TaskProducer<T: TaskInfo>: Send + Sync + std::marker::Sized + 'static + TaskAppCommon<T> {
    async fn send_task(&self, key: &str, param: T::Params) -> anyhow::Result<bool> {
        let collection = self.get_collection();
        let request = TaskRequest {
            key: key.to_string(),
            options: Some(TaskOptions::default()),
            // default to run immediately
            state: TaskState::gen_initial(None),
            param,
        };
        match collection.insert_one(request, None).await {
            Ok(_result) => {
                debug!("task inserted");
                Ok(true)
            }
            Err(e) => {
                match e.kind.as_ref() {
                    ErrorKind::Write(WriteFailure::WriteError(write_error)) => {
                        if write_error.code == 11000 {
                            debug!("task inserted failed, duplicated key");
                            Ok(false)
                        } else {
                            Err(e.into())
                        }
                    }
                    _ => {
                        error!("unknown mongodb error occurred during insert {:?}", &e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}