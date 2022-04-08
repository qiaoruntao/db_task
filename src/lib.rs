extern crate core;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

pub use anyhow;
pub use async_trait;
use mongodb::Client;
pub use mongodb;
use mongodb::options::ClientOptions;
use serde::{Deserialize, Serialize};

use task::{TaskConfig, TaskInfo};

use crate::app::common::TaskAppCommon;
use crate::app::consumer::TaskConsumer;
use crate::app::producer::TaskProducer;
use crate::task::task_options::TaskOptions;
use crate::task::TaskRequest;

pub mod app;
pub mod util;
pub mod task;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TestParams {}


#[derive(Debug)]
struct TestInfo {}

impl TaskInfo for TestInfo {
    type Params = TestParams;
    type Returns = ();
}

struct Test {
    config: TaskConfig,
    concurrency: AtomicUsize,
    collection: mongodb::Collection<TaskRequest<TestInfo>>,
}

impl Test {
    async fn init(connection_str: &str, collection_name: &str) -> Self {
        let mut client_options = ClientOptions::parse(connection_str).await.unwrap();
        let target_database = client_options.default_database.clone().unwrap();
        // Manually set an option.
        client_options.app_name = Some(collection_name.to_string());

        // Get a handle to the deployment.
        let client = Client::with_options(client_options).unwrap();
        let database = client.database(target_database.as_str());
        let collection = database.collection::<TaskRequest<TestInfo>>(collection_name);
        let config: TaskConfig = Default::default();
        let max_concurrency = config.max_concurrency as usize;
        Test {
            config,
            concurrency: AtomicUsize::new(max_concurrency),
            collection,
        }
    }
}

#[async_trait::async_trait]
impl TaskConsumer<TestInfo> for Test {
    fn get_default_option(&'_ self) -> &'_ TaskConfig {
        &self.config
    }

    fn get_concurrency(&'_ self) -> Option<&'_ AtomicUsize> {
        Option::from(&self.concurrency)
    }

    async fn consume(self: Arc<Self>, task: <TestInfo as TaskInfo>::Params) -> anyhow::Result<<TestInfo as TaskInfo>::Returns> {
        println!("consuming {:?}", task);
        tokio::time::sleep(Duration::from_secs(50)).await;
        println!("consumed {:?}", task);

        Ok(())
    }
}


impl TaskAppCommon<TestInfo> for Test {
    fn get_collection(&self) -> &mongodb::Collection<TaskRequest<TestInfo>> {
        &self.collection
    }
}

#[async_trait::async_trait]
impl TaskProducer<TestInfo> for Test {}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use futures::future::join_all;

    use crate::{TaskConsumer, TaskProducer, Test, TestParams};
    use crate::util::test_logger::init_logger;

    #[tokio::test]
    async fn start() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let x = Test::init(connection_str.as_str(), collection_name.as_str()).await;
        let x = Arc::new(x);
        let result = x.start().await;
        dbg!(&result);
    }

    #[tokio::test]
    async fn send_task() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let x = Test::init(connection_str.as_str(), collection_name.as_str()).await;
        let x = Arc::new(x);
        let vec = ('a'..'z')
            .map(|char| {
                let arc = x.clone();
                tokio::spawn(async move {
                    let string = String::from(char);
                    let result = arc.send_task(string.as_str(), TestParams {}).await;
                    dbg!(&result);
                })
            })
            .collect::<Vec<_>>();
        join_all(vec).await;
    }
}
