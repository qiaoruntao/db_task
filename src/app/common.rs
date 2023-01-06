use async_trait::async_trait;
use chrono::Local;
use futures::StreamExt;
use mongodb::bson::{Bson, doc, Document};
use mongodb::Collection;
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::UpdateOptions;
use tracing::{debug, error, instrument};

use crate::task::{TaskInfo, TaskRequest};
use crate::task::task_options::TaskOptions;
use crate::task::task_state::TaskState;

#[async_trait]
pub trait TaskAppCommon<T: TaskInfo> {
    fn get_collection(&self) -> &Collection<TaskRequest<T>>;

    /// check if key is a unique index right now,
    /// key should be unique as we use key to search a specific task
    #[instrument(skip_all,ret)]
    async fn check_collection_index(&self) -> bool {
        let collection = self.get_collection();
        let mut cursor = collection.list_indexes(None).await.unwrap();
        while let Some(Ok(index)) = cursor.next().await {
            if index.options.is_none() {
                continue;
            }
            if let Some(true) = index.options.unwrap().unique {
                if index.keys.contains_key("key") {
                    return true;
                }
            }
        }
        false
    }
}

#[async_trait]
pub trait TaskAppBasicOperations<T: TaskInfo>: TaskAppCommon<T> {
    #[instrument(skip(self,param),ret,err)]
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

    #[instrument(skip(self),err)]
    async fn fetch_task(&self, key: &str) -> anyhow::Result<TaskRequest<T>> {
        let collection = self.get_collection();
        match collection.find_one(doc! {"key":key}, None).await {
            Ok(Some(task)) => {
                anyhow::Ok(task)
            }
            Ok(None) => {
                Err(anyhow::Error::msg("cannot find task"))
            }
            Err(e) => {
                Err(anyhow::Error::new(e))
            }
        }
    }

    // FIXME: this will fully replace the task content(like params), which may have unintended effect
    // returns whether some task is replaced
    #[instrument(skip(self,param),err,ret)]
    async fn replace_task(&self, key: &str, param: T::Params) -> anyhow::Result<bool> {
        let collection = self.get_collection();
        let request = Self::gen_request(key, param);
        let query = doc! {
            "key":key,
            "$or": [
                { "state.next_run_time": { "$lte": chrono::Local::now() } },
                { "state.complete_time": { "$ne": Bson::Null } },
                { "state.cancel_time": { "$ne": Bson::Null } },
            ]
        };
        let mut request_document = mongodb::bson::to_document(&request).expect("cannot serialize");
        // state is generated in the db
        let state = request_document.remove("state").expect("no state in request");
        let update = vec![
            doc! {
                "$replaceWith":{
                    "$mergeObjects": [request_document, {
                        "state": {
                            "$mergeObjects": [
                                state,
                                // the values need to preserve
                                {
                                    // task may be running
                                    "next_run_time": "$$ROOT.state.next_run_time",
                                    "ping_time": "$$ROOT.state.ping_time",
                                    // TODO: do we really need to preserve create time
                                    "create_time": "$$ROOT.state.create_time"
                                }
                            ]
                        }
                    }]
                }
            }
        ];
        debug!("{:?}", &update);
        let mut update_options = UpdateOptions::default();
        update_options.upsert = Some(true);
        match collection.update_one(query, update, Some(update_options)).await {
            Ok(result) => {
                Ok(result.modified_count > 0)
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
        TaskRequest {
            key: key.to_string(),
            options: Some(TaskOptions::default()),
            // default to run immediately
            state: TaskState::gen_initial(None),
            param,
        }
    }

    // send a task with a unique key
    #[instrument(skip_all)]
    async fn send_new_task(&self, param: T::Params) -> anyhow::Result<bool> {
        // FIXME: assume this is unique
        let nanosecond = Local::now().timestamp_nanos();
        self.send_task(format!("{}", nanosecond).as_str(), param).await
    }

    // fail the task with a next retry delay
    #[instrument(skip(self),err)]
    async fn fail_task(&self, key: &String, retry_delay: Option<chrono::Duration>) -> mongodb::error::Result<Option<TaskRequest<T>>> {
        // TODO: calculate delay for failed task
        let delay = retry_delay.unwrap_or_else(|| chrono::Duration::seconds(10));
        let next_run_time = Local::now() + delay;
        let update = doc! {"$set":{"state.prev_fail_time":chrono::Local::now(), "state.next_run_time":next_run_time}};
        let filter = doc! {"key":&key};
        let collection = self.get_collection();
        collection.find_one_and_update(filter, update, None).await
    }

    /// cancel the task
    #[instrument(skip(self), err)]
    async fn cancel_task(&self, key: &String) -> mongodb::error::Result<Option<TaskRequest<T>>> {
        let update = doc! {"$set":{"state.cancel_time":chrono::Local::now(), "state.next_run_time":Bson::Null}};
        let filter = doc! {"key":&key};
        let collection = self.get_collection();
        collection.find_one_and_update(filter, update, None).await
    }

    fn gen_can_run_filter(can_run_now: bool) -> Document {
        let mut filter = Document::default();
        filter.insert("state.success_time", Bson::Null);
        filter.insert("state.cancel_time", Bson::Null);
        if can_run_now {
            filter.insert("state.next_run_time", doc! {"$lte":chrono::Local::now()});
        }
        filter
    }
}