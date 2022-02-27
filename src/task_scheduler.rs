use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use chrono::Local;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use mongodb::{Collection, Cursor};
use mongodb::bson::Bson::Null;
use mongodb::bson::doc;
use mongodb::bson::Document;
use mongodb::bson::oid::ObjectId;
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use qrt_rust_utils::mongodb_manager::mongodb_manager::MongoDbManager;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use crate::task::Task;

#[derive(thiserror::Error, Debug)]
pub enum TaskSchedulerError {
    #[error("task exists")]
    TaskExists,
    #[error("no pending task exists")]
    NoPendingTask,
    // 尝试占用/更新任务的时候可能报这个错
    #[error("no task matched")]
    NoMatchedTask,
    #[error("maintainer error")]
    MaintainerError,
    #[error("task failed")]
    TaskFailedError,
    #[error("cannot occupy task")]
    OccupyTaskFailed,
    #[error("runner wait to exit")]
    RunnerPanic,
    #[error("cannot complete task")]
    CompleteTaskFailed,
    #[error("cannot complete task")]
    CancelTaskFailed,
    #[error(transparent)]
    MongoDbError(#[from] mongodb::error::Error),
    #[error(transparent)]
    UnexpectedError(#[from] anyhow::Error),
}
lazy_static! {
    // TODO: optimize
    static ref GLOBAL_WORKER_ID:i64=Local::now().timestamp();
}
pub struct TaskScheduler {
    db_manager: Mutex<MongoDbManager>,
    task_collection_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MongodbIdEntity {
    _id: ObjectId,
}

pub enum ReInitResult {
    Updated(ObjectId),
    Inserted(ObjectId),
}

impl TaskScheduler {
    pub fn new(db_manager: MongoDbManager, collection_name: String) -> TaskScheduler {
        TaskScheduler {
            db_manager: Mutex::new(db_manager),
            task_collection_name: collection_name,
        }
    }

    pub async fn send_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<ObjectId, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let options = mongodb::options::InsertOneOptions::default();
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let guard = task.try_read().unwrap();
        let task = guard.deref();
        match collection.insert_one(task, options).await {
            Ok(insert_result) => Ok(insert_result.inserted_id.as_object_id().unwrap()),
            Err(e) => {
                match e.kind.as_ref() {
                    ErrorKind::Write(WriteFailure::WriteError(write_error)) => {
                        if write_error.code == 11000 {
                            return Err(TaskSchedulerError::TaskExists);
                        }
                    }
                    _ => {}
                }
                Err(e.into())
            }
        }
    }

    // re-init task if mark as completed, used to fix incorrectly completed task, create task if not exists
    pub async fn re_init_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
        upsert: bool,
    ) -> Result<ReInitResult, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let arc = task.clone();
        let guard = arc.try_read().unwrap();

        let task_guard = guard.deref();
        let pending_filter = TaskScheduler::generate_pending_task_condition(None);
        let filter = doc! {
            "$and":[
                {"$nor":[pending_filter]},
                {"key":&task_guard.key}
            ]
        };
        let mut update_option = FindOneAndUpdateOptions::default();
        update_option.return_document = ReturnDocument::After.into();
        update_option.projection = Some(doc! {
            "_id":1
        });
        let id_only_collection: Collection<MongodbIdEntity> = collection.clone_with_type();

        // let json = serde_json::to_string(&filter).unwrap();
        // println!("{}", &json);
        match id_only_collection.find_one_and_update(filter, doc! {
            "$set":{
                "task_state.start_time": null,
                "task_state.complete_time": null,
            }
        }, Some(update_option)).await {
            Ok(Some(result)) => {
                // dbg!(&result);
                Ok(ReInitResult::Updated(result._id))
            }
            Ok(None) => {
                if upsert {
                    match self.send_task(task).await {
                        Ok(object_id) => {
                            Ok(ReInitResult::Inserted(object_id))
                        }
                        Err(e) => {
                            Err(e.into())
                        }
                    }
                } else {
                    Err(TaskSchedulerError::NoMatchedTask)
                }
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    // get task by key
    pub async fn fetch_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<Task<ParamType, StateType>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let options = mongodb::options::FindOneOptions::default();
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let guard = task.try_read().unwrap();
        let task = guard.deref();
        let filter = doc! {"key":&task.key};
        match collection.find_one(filter, options).await {
            Ok(find_result) => match find_result {
                None => Err(TaskSchedulerError::NoMatchedTask),
                Some(task) => Ok(task),
            },
            Err(e) => Err(e.into()),
        }
    }

    // find tasks that we can process
    pub async fn find_next_pending_task<ParamType, StateType>(
        &self,
        custom_filter: Option<Document>,
    ) -> Result<Arc<RwLock<Task<ParamType, StateType>>>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let result = self
            .find_pending_task::<ParamType, StateType>(custom_filter)
            .await;
        return match result {
            Err(e) => Err(e),
            Ok(mut cursor) => {
                let cursor_result = cursor.try_next().await?;
                match cursor_result {
                    Some(result) => Ok(Arc::new(RwLock::new(result))),
                    None => Err(TaskSchedulerError::NoPendingTask),
                }
            }
        };
    }

    // find tasks that we can process
    pub async fn find_all_pending_task<ParamType, StateType>(
        &self,
        custom_filter: Option<Document>,
    ) -> Result<Vec<Task<ParamType, StateType>>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let result = self
            .find_pending_task::<ParamType, StateType>(custom_filter)
            .await;
        return match result {
            Err(e) => Err(e),
            Ok(cursor) => {
                let cursor_result = cursor
                    .try_collect::<Vec<Task<ParamType, StateType>>>()
                    .await;
                match cursor_result {
                    Ok(result) => Ok(result),
                    Err(e) => Err(e.into()),
                }
            }
        };
    }

    // find tasks that we can process
    pub async fn find_pending_task<ParamType, StateType>(
        &self,
        custom_filter: Option<Document>,
    ) -> Result<Cursor<Task<ParamType, StateType>>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let options = mongodb::options::FindOptions::default();
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());

        let filter = TaskScheduler::generate_pending_task_condition(custom_filter);
        // trace!("&filter={:?}",&filter);
        let cursor = collection.find(filter, Some(options)).await?;
        // trace!("&result={:?}",&result);
        // println!("ok");
        Ok(cursor)
    }

    pub async fn find_task_by_key<ParamType, StateType>(
        &self,
        key: &str,
    ) -> Result<Arc<RwLock<Task<ParamType, StateType>>>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let options = mongodb::options::FindOptions::default();

        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let filter = doc! {"key":key};

        return match collection.find(filter, Some(options)).await {
            Err(e) => Err(e.into()),
            Ok(mut cursor) => {
                let cursor_result = cursor.try_next().await?;
                match cursor_result {
                    Some(result) => Ok(Arc::new(RwLock::new(result))),
                    None => Err(TaskSchedulerError::NoPendingTask),
                }
            }
        };
    }
    //noinspection RsExtraSemicolon
    fn generate_pending_task_condition(custom_filter: Option<Document>) -> Document {
        let mut conditions = vec![
            doc! {"task_state.complete_time":Null},
            doc! {
                "$or":[
                    // not started
                    {"task_state.start_time":Null},
                    // started but not responding
                    {
                        "$and":[
                            {"task_state.next_ping_time":{"$ne":Null}},
                            {"task_state.next_ping_time":{"$lte":mongodb::bson::DateTime::now()}}
                        ]
                    }
                ]
            },
            doc! {"task_state.cancel_time":Null},
        ];
        if let Some(filter) = custom_filter {
            conditions.push(filter)
        }
        doc! {
            "$and":conditions
        }
    }

    // lock the task we want to handle
    pub async fn occupy_pending_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let task = task.try_read().unwrap();
        let basic_filter = TaskScheduler::generate_pending_task_condition(None);
        // we need to make sure the task is still pending
        let filter = doc! {
            "$and":[
                basic_filter,
                {"key":&task.key}
            ]
        };
        let update_pipeline = Self::generate_occupy_update_document();
        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(false);

        let result = collection.update_one(filter, update_pipeline, Some(options)).await?;
        trace!("{:?}",&result);
        if result.matched_count == 0 {
            Err(TaskSchedulerError::NoMatchedTask)
        } else if result.modified_count == 0 {
            Err(TaskSchedulerError::CompleteTaskFailed)
        } else {
            Ok(())
        }
    }

    pub async fn find_and_occupy_pending_task<ParamType, StateType>(
        &self,
        custom_filter: Option<Document>,
    ) -> Result<Arc<RwLock<Task<ParamType, StateType>>>, TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq, {
        let collection = self.db_manager.lock().await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let filter = TaskScheduler::generate_pending_task_condition(custom_filter);
        let update_pipeline = Self::generate_occupy_update_document();
        let mut options = mongodb::options::FindOneAndUpdateOptions::default();
        options.return_document = Some(ReturnDocument::After);
        let result = collection.find_one_and_update(filter, update_pipeline, Some(options)).await?;
        trace!("{:?}",&result);
        match result {
            None => {
                Err(TaskSchedulerError::NoPendingTask)
            }
            Some(task) => {
                Ok(Arc::new(RwLock::from(task)))
            }
        }
    }

    fn generate_occupy_update_document() -> Vec<Document> {
        vec![
            doc! {
                "$set":{
                    "task_state.current_worker_id":&GLOBAL_WORKER_ID.clone(),
                    "task_state.start_time":"$$NOW",
                    "task_state.ping_time":"$$NOW",
                    "task_state.next_ping_time": {
                        "$dateAdd": {
                            "startDate": "$$NOW", "unit": "millisecond", "amount": "$option.ping_interval"
                        }
                    },
                }
            }
        ]
    }

    // update task ping time
    pub async fn update_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());

        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(false);

        let guard = task.try_read().unwrap();
        let task = guard.deref();
        let basic_filter = TaskScheduler::generate_occupied_filter(task);
        let filter = doc! {
            "$and":[
                basic_filter,
                {"key":&task.key}
            ]
        };
        let next_ping_time =
            Local::now() + chrono::Duration::from_std(task.option.ping_interval).unwrap();
        let update = doc! {
            "$set":{
                "task_state.next_ping_time": mongodb::bson::DateTime::from_chrono(next_ping_time),
                "task_state.ping_time": mongodb::bson::DateTime::now(),
            }
        };

        let result = collection.update_one(filter, update, Some(options)).await?;

        if result.matched_count == 0 {
            Err(TaskSchedulerError::NoMatchedTask)
        } else if result.modified_count == 0 {
            Err(TaskSchedulerError::CompleteTaskFailed)
        } else {
            Ok(())
        }
    }

    fn generate_occupied_filter<ParamType, StateType>(
        task: &Task<ParamType, StateType>,
    ) -> Document {
        doc! {
            "task_state.complete_time": mongodb::bson::Bson::Null,
            // "task_state.current_worker_id": &GLOBAL_WORKER_ID.clone(),
            "task_state.next_ping_time": { "$gte": mongodb::bson::DateTime::now() },
            "task_state.ping_time": { "$lte": mongodb::bson::DateTime::now() },
            "key":&task.key
        }
    }

    // mark task as completed
    pub async fn complete_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let guard = task.try_read().unwrap();
        let task = guard.deref();
        // we need to make sure the task is being processed by ourself
        let filter = TaskScheduler::generate_occupied_filter(task);
        let update = doc! {
            "$set":{
                "task_state.complete_time":mongodb::bson::DateTime::now(),
                "task_state.current_worker_id":mongodb::bson::Bson::Null,
                "task_state.next_ping_time": mongodb::bson::Bson::Null,
            }
        };

        trace!("filter={:?}",&filter);
        trace!("update={:?}",&update);
        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(false);

        let result = collection.update_one(filter, update, Some(options)).await?;
        trace!("result={:?}",&result);
        if result.matched_count == 0 {
            Err(TaskSchedulerError::NoMatchedTask)
        } else if result.modified_count == 0 {
            Err(TaskSchedulerError::CompleteTaskFailed)
        } else {
            Ok(())
        }
    }

    // mark task as cancelled
    pub async fn cancel_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let guard = task.try_read().unwrap();
        let task = guard.deref();
        // we need to make sure the task is being processed by ourself
        let filter = doc! {
            "task_state.complete_time": mongodb::bson::Bson::Null,
            "task_state.cancel_time": mongodb::bson::Bson::Null,
            "task_state.current_worker_id": &task.task_state.current_worker_id,
            "key":&task.key
        };
        let update = doc! {
            "$set":{
                "task_state.cancel_time":mongodb::bson::DateTime::now(),
                "task_state.current_worker_id":mongodb::bson::Bson::Null,
                "task_state.next_ping_time": mongodb::bson::Bson::Null,
            }
        };

        trace!("&filter={:?}",&filter);
        trace!("&update={:?}",&update);
        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(false);

        let result = collection.update_one(filter, update, Some(options)).await?;
        trace!("&result={:?}",&result);
        if result.matched_count == 0 {
            Err(TaskSchedulerError::NoMatchedTask)
        } else if result.modified_count == 0 && result.upserted_id == None {
            Err(TaskSchedulerError::CancelTaskFailed)
        } else {
            Ok(())
        }
    }

    // mark task as cancelled
    pub async fn fail_task<ParamType, StateType>(
        &self,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError>
        where
            ParamType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
            StateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    {
        let collection = self
            .db_manager
            .lock()
            .await
            .get_collection::<Task<ParamType, StateType>>(self.task_collection_name.as_str());
        let guard = task.try_read().unwrap();
        let task = guard.deref();
        // we need to make sure the task is being processed by ourself
        let filter = doc! {
            "task_state.complete_time": mongodb::bson::Bson::Null,
            "task_state.cancel_time": mongodb::bson::Bson::Null,
            // TODO: add restriction if possible
            // "task_state.current_worker_id": &task.task_state.current_worker_id,
            "key":&task.key
        };
        let update = vec![
            doc! {
                "$set":{
                    "task_state.previous_fail_time":"$$NOW",
                    "task_state.retry_time_left":{
                        "$add": [
                            -1,{"$ifNull":["$task_state.retry_time_left",1]}
                        ]
                    },
                    "task_state.current_worker_id":mongodb::bson::Bson::Null,
                    "task_state.next_ping_time": mongodb::bson::Bson::Null,
                }
            }
        ];

        trace!("&filter={:?}",&filter);
        trace!("&update={:?}",&update);
        let mut options = mongodb::options::UpdateOptions::default();
        options.upsert = Some(false);

        let result = collection.update_one(filter, update, Some(options)).await?;
        trace!("&result={:?}",&result);
        if result.matched_count == 0 {
            Err(TaskSchedulerError::NoMatchedTask)
        } else if result.modified_count == 0 && result.upserted_id == None {
            Err(TaskSchedulerError::CancelTaskFailed)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test_task_scheduler {
    use std::sync::Arc;

    use chrono::Local;
    use lazy_static::lazy_static;
    use qrt_rust_utils::config_manage::config_manager::ConfigManager;
    use qrt_rust_utils::logger::logger::{Logger, LoggerConfig};
    use qrt_rust_utils::mongodb_manager::mongodb_manager::MongoDbManager;
    use tokio::sync::RwLock;
    use tracing::trace;

    use crate::task::{DefaultTaskState, Task, TaskMeta, TaskOptions, TaskState};
    use crate::task_scheduler::TaskScheduler;

    lazy_static! {
        static ref COLLECTION_NAME: String = "live_record".into();
    }

    #[tokio::test]
    async fn send_task() {
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        let name = Local::now().timestamp().to_string();
        let task_options = TaskOptions::default();
        let task_state = TaskState::from(&task_options);
        let task = Task {
            key: name,
            meta: TaskMeta {
                name: "test".to_string(),
                create_time: Local::now(),
                creator: "default".to_string(),
            },
            option: task_options,
            task_state,
            param: 1,
            state: DefaultTaskState::default(),
        };
        let send_result = scheduler.send_task(Arc::new(RwLock::new(task))).await;
        trace!("&send_result={:?}",&send_result);
    }

    #[tokio::test]
    async fn find_pending_task() {
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        tokio::spawn(async move {
            let result1 = scheduler.find_pending_task::<i32, i32>(None).await.unwrap();
            println!("{:?}", &result1);
        });
    }

    #[tokio::test]
    async fn complete_pending_task() {
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        let arc = scheduler.find_and_occupy_pending_task::<i32, i32>(None).await.unwrap();
        let complete_result = scheduler.complete_task(arc).await;
        trace!("&result1={:?}",&complete_result);
    }

    #[tokio::test]
    async fn cancel_pending_task() {
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        let arc = scheduler.find_and_occupy_pending_task::<i32, i32>(None).await.unwrap();
        let complete_result = scheduler.cancel_task(arc).await;
        trace!("&complete_result={:?}",&complete_result);
    }

    #[tokio::test]
    async fn fail_pending_task() {
        let logger_config = LoggerConfig {
            level: "trace".to_string()
        };
        Logger::init_logger(&logger_config);
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        let arc = scheduler.find_and_occupy_pending_task::<i32, i32>(None).await.unwrap();
        let result = scheduler.fail_task(arc).await;
        trace!("&complete_result={:?}",&result);
    }

    #[tokio::test]
    async fn re_init_task() {
        let logger_config = LoggerConfig {
            level: "trace".to_string()
        };
        Logger::init_logger(&logger_config);
        let result = ConfigManager::read_config_with_directory("./config/mongo").unwrap();
        let db_manager = MongoDbManager::new(result, "Logger").unwrap();
        let scheduler = TaskScheduler::new(db_manager, COLLECTION_NAME.clone());
        let arc = scheduler.find_task_by_key::<i32, i32>("aaa").await.unwrap();
        arc.try_write().unwrap().key = "aaa".parse().unwrap();
        let result = scheduler.re_init_task(arc, true).await;
        trace!("&complete_result={:?}",&result);
    }
}
