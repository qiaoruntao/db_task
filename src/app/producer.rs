use async_trait::async_trait;

use crate::app::common::{TaskAppBasicOperations, TaskAppCommon};
use crate::task::TaskInfo;

#[async_trait]
pub trait TaskProducer<T: TaskInfo>: Send + Sync + Sized + 'static + TaskAppCommon<T> + TaskAppBasicOperations<T> {}