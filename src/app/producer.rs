use std::sync::Arc;

use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::UpdateOptions;
use tracing::{debug, error};

use crate::{TaskInfo, TaskOptions};
use crate::app::common::TaskAppCommon;
use crate::task::task_state::TaskState;
use crate::task::TaskRequest;

#[async_trait]
pub trait TaskProducer<T: TaskInfo>: Send + Sync + std::marker::Sized + 'static + TaskAppCommon<T> {
    async fn send_task(&self, key: &str, param: T::Params) -> anyhow::Result<bool> {
        let collection = self.get_collection();
        let request = Self::gen_request(key, param);
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

    // FIXME: this will fully replace the task content(like params), which may have unintended effect
    async fn replace_task(&self, key: &str, param: T::Params) -> anyhow::Result<bool> {
        let collection = self.get_collection();
        let request = Self::gen_request(key, param);
        let query = doc! {"key":key};
        let mut request_document = mongodb::bson::to_document(&request).expect("cannot serialize");
        // state is generated in the db
        let state = request_document.remove("state").expect("no state in request");
        let update = vec![
            doc!{
                "$replaceWith":{
                    "$mergeObjects": [request_document, {
                        "state": {
                            "$mergeObjects": [state, "$$ROOT.state", {
                                // task may be running
                                "next_run_time": "$$ROOT.state.next_run_time",
                                // TODO: do we really need to preserve create time
                                "create_time": "$$ROOT.state.create_time"
                            }]
                        }
                    }]
                }
            }
        ];
        let mut update_options = UpdateOptions::default();
        update_options.upsert = Some(true);
        match collection.update_one(query, update, Some(update_options)).await {
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

    fn gen_request(key: &str, param: <T as TaskInfo>::Params) -> TaskRequest<T> {
        let request = TaskRequest {
            key: key.to_string(),
            options: Some(TaskOptions::default()),
            // default to run immediately
            state: TaskState::gen_initial(None),
            param,
        };
        request
    }

    // send a task with a unique key
    async fn send_new_task(&self, param: T::Params) -> anyhow::Result<bool> {
        // FIXME: assume this is unique
        let nanosecond = chrono::Local::now().timestamp_nanos();
        self.send_task(format!("{}", nanosecond).as_str(), param).await
    }
}