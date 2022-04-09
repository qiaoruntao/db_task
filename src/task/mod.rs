use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use task_options::TaskOptions;
use task_state::TaskState;

pub mod task_options;
pub mod task_state;

pub struct TaskConfig {
    pub global_options: TaskOptions,
    pub max_concurrency: u32,
}

impl Default for TaskConfig {
    fn default() -> Self {
        TaskConfig {
            global_options: Default::default(),
            max_concurrency: 5,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskRequest<T> where T: TaskInfo {
    pub key: String,
    pub options: Option<TaskOptions>,
    pub state: TaskState,
    pub param: T::Params,
}

pub trait TaskInfo: Send + Sync + 'static {
    /// The parameters of the task.
    type Params: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Unpin + Debug + 'static;

    /// The return type of the task.
    type Returns: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Unpin + Debug + 'static;
}
