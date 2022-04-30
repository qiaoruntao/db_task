use async_trait::async_trait;
use futures::StreamExt;
use mongodb::Collection;
use crate::task::{TaskInfo, TaskRequest};


#[async_trait]
pub trait TaskAppCommon<T: TaskInfo> {
    fn get_collection(&self) -> &Collection<TaskRequest<T>>;

    /// check if key is a unique index right now,
    /// key should be unique as we use key to search a specific task
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