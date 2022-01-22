use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use mongodb::bson::Document;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, trace};
use crate::consumer_config::ConsumerConfig;

use crate::task::Task;
use crate::task_scheduler::{TaskScheduler, TaskSchedulerError};

pub enum TaskConsumerResult {
    Completed,
    Cancelled,
    Failed,
    RequestStop,
}

pub trait TaskParamType:
Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq
{}

pub trait TaskStateType: Debug + Serialize + DeserializeOwned + Unpin + Sync + Send {}

#[async_trait]
pub trait TaskConsumer<
    ParamType: 'static + Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
    StateType: 'static + Debug + Serialize + DeserializeOwned + Unpin + Sync + Send + PartialEq,
>
{
    fn build_filter(&self) -> Option<Document>;
    // define how to consume a task
    async fn consume(&self, task: Arc<RwLock<Task<ParamType, StateType>>>) -> TaskConsumerResult;
    // used to run background maintain
    async fn add_running_task(&self, task: Arc<RwLock<Task<ParamType, StateType>>>) -> bool;
    async fn remove_running_task(&self, task: Arc<RwLock<Task<ParamType, StateType>>>) -> bool;
    // maintain task
    async fn run_maintainer(&self, task_scheduler: Arc<RwLock<TaskScheduler>>);

    async fn run_task_core(
        &self,
        task_scheduler: Arc<RwLock<TaskScheduler>>,
        task: Arc<RwLock<Task<ParamType, StateType>>>,
    ) -> Result<(), TaskSchedulerError> {
        let task_scheduler_guard = task_scheduler.try_read().unwrap();
        // background maintainer
        let key = &task.try_read().unwrap().key;
        let is_added = self.add_running_task(task.clone()).await;
        if !is_added {
            error!("task {} already running", &key);
            return Err(TaskSchedulerError::MaintainerError);
        }
        // run the task
        let consumer_result = self.consume(task.clone());
        // update result
        let result = match consumer_result.await {
            TaskConsumerResult::Completed => task_scheduler_guard.complete_task(task.clone()).await,
            TaskConsumerResult::Cancelled => task_scheduler_guard.cancel_task(task.clone()).await,
            TaskConsumerResult::Failed => task_scheduler_guard.fail_task(task.clone()).await,
            TaskConsumerResult::RequestStop => {
                Err(TaskSchedulerError::RunnerPanic)
            }
        };
        // remove maintainer
        let is_removed = self.remove_running_task(task.clone()).await;
        if !is_removed {
            error!("task {} not exists", &key);
            return Err(TaskSchedulerError::MaintainerError);
        }
        result
    }
    async fn main_loop<T: 'static + TaskConsumer<ParamType, StateType> + Sync + Send>(
        arc_scheduler: Arc<RwLock<TaskScheduler>>,
        arc_consumer: Arc<RwLock<T>>,
        consumer_config: ConsumerConfig,
    ) {
        info!("start to run main loop");
        let semaphore = Arc::new(Semaphore::new(consumer_config.max_concurrency as usize));
        let duration = consumer_config.task_delay;
        let running_state = Arc::new(AtomicBool::new(true));
        while running_state.load(Ordering::Relaxed) {
            // try get token now
            let arc1 = arc_consumer.clone();
            let consumer = arc1.try_read().unwrap();
            let filter = consumer.build_filter();
            // maintain
            let arc2 = arc_scheduler.clone();
            consumer.run_maintainer(arc2.clone()).await;
            drop(consumer);
            let token = match semaphore.clone().try_acquire_owned() {
                Ok(token) => { token }
                Err(_) => {
                    trace!("cannot acquire a token");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            // find a task
            let task_scheduler_guard = arc2.try_read().unwrap();

            let result = task_scheduler_guard.find_and_occupy_pending_task(filter.clone()).await;
            let task = match result {
                Err(TaskSchedulerError::NoPendingTask) => {
                    trace!("no pending task found");
                    // we have to wait for a while and then check
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => {
                    // unexpected error
                    error!("{:?}", e);
                    break;
                }
                Ok(value) => {
                    value
                }
            };

            drop(task_scheduler_guard);
            let state = running_state.clone();
            // send to background
            tokio::spawn(async move {
                let arc = arc1;
                let consumer = arc.try_read().unwrap();
                match consumer.run_task_core(arc2, task.clone()).await {
                    Ok(_) => {}
                    Err(TaskSchedulerError::RunnerPanic) => {
                        error!("runner panic, stop executing now");
                        state.store(false, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let read_guard = task.try_read().unwrap();
                        println!("running task failed, {:?}, task is {:?}", &e, &read_guard);
                    }
                }
                if !duration.is_zero() {
                    trace!("sleep {:?} after task finished", duration);
                    tokio::time::sleep(duration).await;
                }
                drop(token);
            });
        }
    }
}
