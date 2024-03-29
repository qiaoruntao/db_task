use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Local};
use futures::{FutureExt, StreamExt};
use mongodb::bson::{Bson, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::Collection;
use mongodb::options::{ChangeStreamOptions, FindOneAndUpdateOptions, FindOneOptions, FullDocumentType, ReturnDocument};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, error, info, instrument, warn};

use crate::app::common::{TaskAppBasicOperations, TaskAppCommon};
use crate::task::{TaskConfig, TaskInfo, TaskRequest};
use crate::task::task_options::TaskOptions;
use crate::task::task_state::TaskStateNextRunTime;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TaskInfoNextRunTime {
    state: TaskStateNextRunTime,
}

#[async_trait]
pub trait TaskConsumeFunc<T: TaskInfo>: Send + Sync + Sized + 'static {
    async fn consume(self: Arc<Self>, params: <T as TaskInfo>::Params) -> anyhow::Result<<T as TaskInfo>::Returns>;
}

#[async_trait]
pub trait TaskConsumeCore<T: TaskInfo>: Send + Sync + Sized + 'static + TaskAppCommon<T> + TaskAppBasicOperations<T> {
    fn get_default_option(&'_ self) -> &'_ TaskConfig;
    fn get_concurrency(&'_ self) -> Option<&'_ AtomicUsize>;
}

#[async_trait]
pub trait TaskConsumer<T: TaskInfo>: TaskConsumeFunc<T> + TaskConsumeCore<T> {
    /// how the client handle the task
    #[instrument(skip(self))]
    async fn handle_execution_result(self: Arc<Self>, result: anyhow::Result<T::Returns>, key: String, retry_delay: Option<chrono::Duration>) -> anyhow::Result<bool> {
        let is_success = result.is_ok();
        // TODO: store the result?
        let update_result = match result {
            Ok(_returns) => {
                let filter = doc! {"key":&key};
                let update = doc! {"$set":{"state.success_time":chrono::Local::now()}};
                let collection = self.get_collection();
                collection.find_one_and_update(filter, update, None).await
            }
            Err(e) => {
                error!("execution failed, e={}",e);
                self.fail_task(&key, retry_delay).await
            }
        };
        let state = if is_success {
            "success"
        } else {
            "failed"
        };
        match update_result {
            Ok(Some(_result)) => {
                debug!("task state updated, key={}, state is {}", key, state);
                Ok(true)
            }
            Ok(None) => {
                let msg = format!("failed to update task state, key={}, should be {}", key, state);
                error!("{}",msg);
                Err(anyhow::Error::msg(msg))
            }
            Err(e) => {
                error!("unknown mongodb error occurred during insert {:?}", &e);
                Err(e.into())
            }
        }
    }

    #[instrument(skip(self))]
    async fn handle_wait_task_sleep(self: Arc<Self>, key: String, chrono_duration: chrono::Duration) -> anyhow::Result<()> {
        debug!("updating ping for task key={}",key);
        // update task state
        let now = Local::now();
        // allow fail twice
        let next_run_time = now + chrono_duration + chrono_duration + chrono_duration;
        let identifier = doc! {
                        "key":&key,
                        "state.success_time":Bson::Null,
                        "state.cancel_time":Bson::Null,
                        "state.ping_time":{
                            "$lt":now
                        }
                    };
        let update = doc! {
                        "$set":{
                            "state.next_run_time":next_run_time,
                            "state.ping_time":now
                        }
                    };
        let collection = self.get_collection();
        match collection.find_one_and_update(identifier, update, None).await {
            Ok(Some(_)) => {
                debug!("ping time updated, key={}",key);
                Ok(())
            }
            Ok(None) => {
                let msg = format!("failed to update ping time, key={}", key);
                error!("{}",msg);
                Err(anyhow::Error::msg(msg))
            }
            Err(e) => {
                error!("unknown mongodb error occurred during insert {:?}", &e);
                Err(e.into())
            }
        }
    }
    /// how the consumer handle the task
    /// TODO: detect worker conflict
    #[instrument(skip(self))]
    async fn consume_task(self: Arc<Self>, key: String, params: T::Params, options: TaskOptions) {
        // TODO: the task state should be already updated, we start maintain work here
        let mut task_execution = self.clone().consume(params)
            .into_stream();
        let chrono_duration = options.ping
            .unwrap_or_else(|| chrono::Duration::seconds(5));
        let duration = chrono_duration
            .to_std().unwrap();
        loop {
            tokio::select! {
                _=tokio::time::sleep(duration)=>{
                    tokio::spawn(self.clone().handle_wait_task_sleep(key.clone(),chrono_duration));
                }
                // TODO: how to consume it only once without stream?
                Some(execution_result)=task_execution.next()=>{
                    let _result=self.handle_execution_result(execution_result, key, options.min_retry_delay).await;
                    break;
                }
                // TODO: add task timeout
            }
        }
    }

    async fn handle_change_stream(self: Arc<Self>, event: ChangeStreamEvent<TaskStateNextRunTime>) -> Option<DateTime<Local>> {
        debug!("handle_change_stream");
        event.full_document.map(|doc| doc.next_run_time)
    }

    #[instrument(skip(self), ret, err)]
    async fn fetch_next_run_time(self: Arc<Self>) -> anyhow::Result<Option<DateTime<Local>>> {
        debug!("fetch_next_run_time");
        let collection = self.get_collection();
        let filter = doc! {
            "state.success_time":Bson::Null,
            "state.cancel_time":Bson::Null,
            "state.next_run_time":{
                "$gte":chrono::Local::now()
            }
        };
        let mut find_one_options = FindOneOptions::default();
        find_one_options.projection = Some(doc! {
            "state.next_run_time":1_i32
        });
        find_one_options.sort = Some(doc! {
            "state.next_run_time":1_i32
        });
        match collection.clone_with_type::<TaskInfoNextRunTime>().find_one(filter, Some(find_one_options)).await {
            Ok(Some(task_info)) => {
                Ok(Option::from(task_info.state.next_run_time))
            }
            Err(e) => {
                error!("{:?}",e);
                Err(e.into())
            }
            _ => {
                Ok(None)
            }
        }
    }

    #[instrument(skip_all, fields(collection_name = collection.name(), task_key))]
    async fn search_and_occupy(self: Arc<Self>, collection: &Collection<TaskRequest<T>>) -> Option<TaskRequest<T>> {
        let filter = Self::gen_can_run_filter(true);
        let update = doc! {
            "$set":{
                "state.next_run_time":chrono::Local::now()+self.get_default_option().global_options.ping.unwrap()*2,
                "state.ping_time":chrono::Local::now(),
            }
        };
        let mut options = FindOneAndUpdateOptions::default();
        options.return_document = Option::from(ReturnDocument::After);
        let result = match collection.find_one_and_update(filter, update, Some(options)).await {
            Ok(value) => {
                value
            }
            Err(e) => {
                dbg!(&e);
                None
            }
        };
        let _task_key = result.as_ref().map(|result| result.key.clone());
        result
    }

    #[instrument(skip_all, fields(collection_name = collection.name()))]
    async fn handle_sleep(self: Arc<Self>, collection: &Collection<TaskRequest<T>>, is_changed: &AtomicBool, tasks: Arc<Mutex<HashSet<String>>>) -> Option<DateTime<Local>> {
        debug!("handle_sleep");
        if let Some(concurrency) = self.get_concurrency() {
            // try acquire concurrency
            for _ in 0..10 {
                let current = concurrency.load(Ordering::SeqCst);
                if current > 0 {
                    let next_current = current - 1;
                    if concurrency.compare_exchange(current, next_current, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                        // decreased
                        debug!("concurrency decreased to {}", next_current);
                        break;
                    }
                } else {
                    // concurrency limited
                    return None;
                }
            }
        }
        // if we don't have concurrency, we won't search
        let task = self.clone().search_and_occupy(collection).await;
        let arc = self.clone();
        let add_task = async move {
            if let Some(currency) = arc.get_concurrency() {
                // release when task completed
                debug!("concurrency increased");
                currency.fetch_add(1, Ordering::SeqCst);
            }
        };
        if let Some(task) = task {
            debug!("new task found");
            let next_run_time = task.state.next_run_time;
            let max_allowed_time = next_run_time + chrono::Duration::seconds(10);

            let now = Local::now();
            if max_allowed_time < now {
                // TODO: for test purpose, check if some task is delayed
                warn!("task delayed key={} next_run_time is {}, now is {}", &task.key, &next_run_time, &now);
            }
            // handle the task
            tokio::spawn(async move {
                let key = task.key;
                let is_inserted = tasks.try_lock().unwrap().insert(key.clone());
                if !is_inserted {
                    // task duplicated, should not consume it
                    error!("cannot insert current task key {}", &key);
                    add_task.await;
                    return;
                }
                self.clone().consume_task(key.clone(), task.param, task.options.unwrap_or_default()).await;
                // when exiting, we cannot lock the tasks
                if let Ok(mut handler) = tasks.try_lock() {
                    let is_removed = handler.remove(&key);
                    if !is_removed {
                        error!("cannot remove current task key {}", &key);
                    }
                    add_task.await;
                }
            });
            // cannot infer a correct next-run-time right now, try occupy again
            return Some(Local::now());
        } else {
            debug!("no task found");
            // immediately use it
            add_task.await;
        }

        if !is_changed.load(Ordering::SeqCst) {
            // no need to check now
            return None;
        }
        // no pending task now
        is_changed.store(false, Ordering::SeqCst);
        match self.fetch_next_run_time().await {
            Ok(time) => {
                time
            }
            _ => {
                None
            }
        }
    }

    async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        // TODO: need to check before send task
        if !self.check_collection_index().await {
            return Err(anyhow::Error::msg("unique index is not set"));
        }
        let collection = self.get_collection();
        let can_run_filter = Self::gen_can_run_filter(false);
        // very helpful resource
        // https://www.percona.com/blog/2018/03/07/using-mongodb-3-6-change-streams/
        let pipeline = [
            // remove unnecessary fields
            doc! {
                "$addFields":{
                    "fullDocument":"$fullDocument.state",
                    "state":"$fullDocument.state"
                }
            },
            doc! {
                "$match":can_run_filter
            },
            doc! {
                "$project":{
                    // _id cannot get filtered, will get error if filtered
                    "operationType":1_i32,
                    // mongodb-rust says ns field should not get filtered
                    "ns":1_i32,
                    "fullDocument.next_run_time":1_i32
                }
            }
        ];
        let mut change_stream_options = ChangeStreamOptions::default();
        change_stream_options.full_document = Some(FullDocumentType::UpdateLookup);
        // TODO: how actually does it work?
        change_stream_options.max_await_time = Some(Duration::from_secs(10));
        let watch_collection = collection.clone_with_type::<TaskStateNextRunTime>();
        let mut change_stream = match watch_collection.watch(pipeline, Some(change_stream_options)).await {
            Ok(value) => { value }
            Err(e) => {
                dbg!(&e);
                return Err(e.into());
            }
        };
        debug!("change stream listening");
        let mut wakeup_time = tokio::time::Instant::now();
        let running_tasks = Arc::new(Mutex::new(HashSet::new()));
        // whether remote dataset has changed, if not changed, we don't need to fetch anything from db
        let is_changed = AtomicBool::new(true);
        loop {
            tokio::select! {
                _=tokio::signal::ctrl_c()=>{
                    // stop the whole consumer
                    info!("consumer stopping");
                    break;
                }
                _=tokio::time::sleep_until(wakeup_time)=>{
                    // will try to occupy and execute task
                    if let Some(next_check_time)=self.clone().handle_sleep(collection,&is_changed, running_tasks.clone()).await{
                        debug!("next_check_time={}", &next_check_time);
                        wakeup_time = tokio::time::Instant::now()+(next_check_time-chrono::Local::now()).to_std()
                        .unwrap_or(Duration::ZERO);
                    }else{
                        debug!("next_check_time=None");
                        // nothing happened, we can sleep for a very long time until change stream notifies us
                        wakeup_time=tokio::time::Instant::now()
                        +self.get_default_option().global_options.ping
                        .map(|time|time*10)
                        .unwrap_or_else(|| chrono::Duration::seconds(10))
                        .to_std().unwrap();
                    }
                }
                // None doesn't seem to exist
                Some(stream_event)=change_stream.next()=>{
                    // change stream is only used to detect a better next run time now
                    debug!("{:#?}",&stream_event);
                    if let Ok(event)=stream_event{
                        debug!("mark as changed stream");
                        is_changed.store(true, Ordering::SeqCst);
                        let this_check_time = match self.clone().handle_change_stream(event).await{
                            Some(value)=>value,
                            None=>{
                                continue;
                            }
                        };
                        let this_wakeup_time = tokio::time::Instant::now()+(this_check_time-chrono::Local::now()).to_std().unwrap_or(Duration::ZERO);
                        wakeup_time=this_wakeup_time.min(wakeup_time);
                    }
                }
            }
        }
        info!("exiting");
        // no new task will be occupied and execute now
        for key in running_tasks.try_lock().unwrap().iter() {
            // no retry delay so any other consumer can pick task up immediately
            let fail_result = self.fail_task(key, Some(chrono::Duration::zero())).await;
            if fail_result.is_err() {
                error!("failed to fail task key={}", key);
            }
        }
        info!("mark all task as failed, quit now");
        Ok(())
    }
}