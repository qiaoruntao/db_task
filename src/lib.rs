extern crate core;


pub use anyhow;
pub use async_trait;
pub use mongodb;

pub mod app;
pub mod util;
pub mod task;
pub mod tasker;


#[cfg(test)]
pub mod tests {
    use std::env;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use futures::future::join_all;
    use mongodb::Client;
    use mongodb::options::ClientOptions;
    use serde::{Deserialize, Serialize};
    use tracing::{error, info};
    use tracing::log::debug;

    use crate::app::common::{TaskAppBasicOperations, TaskAppCommon};
    use crate::app::consumer::{TaskConsumeCore, TaskConsumeFunc, TaskConsumer};
    use crate::app::producer::TaskProducer;
    use crate::task::{TaskConfig, TaskInfo};
    use crate::task::TaskRequest;
    use crate::util::test_logger::tests::init_logger;

    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct TestParams {}


    #[derive(Debug)]
    struct TestInfo {}

    impl TaskInfo for TestInfo {
        type Params = TestParams;
        type Returns = ();
    }

    pub struct Test {
        config: TaskConfig,
        concurrency: AtomicUsize,
        collection: mongodb::Collection<TaskRequest<TestInfo>>,
    }

    impl Test {
        pub(crate) async fn init(connection_str: &str, collection_name: &str) -> Self {
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
    impl TaskConsumeFunc<TestInfo> for Test {
        async fn consume(self: Arc<Self>, task: <TestInfo as TaskInfo>::Params) -> anyhow::Result<<TestInfo as TaskInfo>::Returns> {
            println!("consuming {:?}", task);
            tokio::time::sleep(Duration::from_secs(50)).await;
            println!("consumed {:?}", task);

            Ok(())
        }
    }

    impl TaskConsumeCore<TestInfo> for Test {
        fn get_default_option(&'_ self) -> &'_ TaskConfig {
            &self.config
        }

        fn get_concurrency(&'_ self) -> Option<&'_ AtomicUsize> {
            Option::from(&self.concurrency)
        }
    }

    #[async_trait::async_trait]
    impl TaskConsumer<TestInfo> for Test {}

    #[async_trait::async_trait]
    impl TaskAppBasicOperations<TestInfo> for Test {}

    impl TaskAppCommon<TestInfo> for Test {
        fn get_collection(&self) -> &mongodb::Collection<TaskRequest<TestInfo>> {
            &self.collection
        }
    }

    #[async_trait::async_trait]
    impl TaskProducer<TestInfo> for Test {}

    #[tokio::test]
    async fn start() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let x = Test::init(connection_str.as_str(), collection_name.as_str()).await;
        let x = Arc::new(x);
        tokio::select! {
            Err(e)=x.start()=>{
                error!("failed to start consumer, e={}", e);
            }
            _=tokio::time::sleep(tokio::time::Duration::from_secs(10))=>{
                info!("consumer has run for 10 seconds, seems ok");
            }
        }
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
                    info!("{:?}",&result);
                })
            })
            .collect::<Vec<_>>();
        join_all(vec).await;
    }

    #[tokio::test]
    async fn replace_task() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let x = Test::init(connection_str.as_str(), collection_name.as_str()).await;
        let arc = Arc::new(x);
        // create a new task for test
        let timestamp = chrono::Local::now().timestamp().to_string();
        let send_result = arc.send_task(timestamp.as_str(), TestParams {}).await;
        if send_result.is_err() {
            error!("failed to send task");
        }
        let fetch_result = arc.fetch_task(timestamp.as_str()).await;
        if let Ok(task) = fetch_result {
            debug!("{:?}",&task);
        }
        if let Ok(result) = arc.fail_task(&timestamp, None).await {
            debug!("{:?}",&result);
        }
        let replace_result = arc.replace_task(timestamp.as_str(), TestParams {}).await;
        if replace_result.is_err() {
            error!("failed to send task");
        }
        let fetch_result = arc.fetch_task(timestamp.as_str()).await;
        if let Ok(task) = fetch_result {
            debug!("{:?}",&task);
        }
    }

    #[tokio::test]
    async fn test_speed() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let x = Arc::new(Test::init(connection_str.as_str(), collection_name.as_str()).await);
        tokio::spawn(x.start());
        let test_cnt = 1000;
        for index in 0..test_cnt {}
    }
}
