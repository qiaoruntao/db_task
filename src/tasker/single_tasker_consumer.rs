use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use mongodb::{Client, Collection};
use mongodb::options::{ClientOptions, ResolverConfig};
use tracing::info;

use crate::app::common::{TaskAppBasicOperations, TaskAppCommon};
use crate::app::consumer::{TaskConsumeCore, TaskConsumeFunc, TaskConsumer};
use crate::task::{TaskConfig, TaskInfo, TaskRequest};

// Given a collection and consume method, handle all the rest work of TaskConsumer generation
pub struct SingleTaskerConsumer<T: TaskInfo, K: TaskConsumeFunc<T>> {
    config: TaskConfig,
    concurrency: Option<AtomicUsize>,
    collection: Collection<TaskRequest<T>>,
    consumer: Arc<K>,
}

impl<T: TaskInfo, K: TaskConsumeFunc<T>> SingleTaskerConsumer<T, K> {
    pub async fn init(connection_str: &str, collection_name: &str, consumer: K) -> Self {
        let mut client_options = if cfg!(windows) && connection_str.contains("+srv") {
            info!("test");
            ClientOptions::parse_with_resolver_config(connection_str, ResolverConfig::quad9()).await.unwrap()
        } else {
            ClientOptions::parse(connection_str).await.unwrap()
        };
        let target_database = client_options.default_database.clone().unwrap();
        // Manually set an option.
        client_options.app_name = Some(collection_name.to_string());

        // Get a handle to the deployment.
        let client = Client::with_options(client_options).unwrap();
        let database = client.database(target_database.as_str());
        let collection = database.collection(collection_name);
        SingleTaskerConsumer {
            config: Default::default(),
            concurrency: None,
            collection,
            consumer: Arc::new(consumer),
        }
    }
}

impl<T: TaskInfo, K: TaskConsumeFunc<T>> TaskAppCommon<T> for SingleTaskerConsumer<T, K> {
    fn get_collection(&self) -> &Collection<TaskRequest<T>> {
        &self.collection
    }
}

#[async_trait::async_trait]
impl<T: TaskInfo, K: TaskConsumeFunc<T>> TaskConsumeFunc<T> for SingleTaskerConsumer<T, K> {
    async fn consume(self: Arc<Self>, params: <T as TaskInfo>::Params) -> anyhow::Result<<T as TaskInfo>::Returns> {
        self.consumer.clone().consume(params).await
    }
}

impl<T: TaskInfo, K: TaskConsumeFunc<T>> TaskAppBasicOperations<T> for SingleTaskerConsumer<T, K> {}

#[async_trait::async_trait]
impl<T: TaskInfo, K: TaskConsumeFunc<T>> TaskConsumeCore<T> for SingleTaskerConsumer<T, K> {
    fn get_default_option(&'_ self) -> &'_ TaskConfig {
        &self.config
    }

    fn get_concurrency(&'_ self) -> Option<&'_ AtomicUsize> {
        self.concurrency.as_ref()
    }
}

impl<T: TaskInfo, K: TaskConsumeFunc<T>> TaskConsumer<T> for SingleTaskerConsumer<T, K> {}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use crate::app::consumer::{TaskConsumeFunc, TaskConsumer};
    use crate::task::TaskInfo;
    use crate::tasker::single_tasker_consumer::SingleTaskerConsumer;
    use crate::util::test_logger::tests::init_logger;

    struct TestA {}

    impl TaskInfo for TestA {
        type Params = ();
        type Returns = ();
    }

    struct RunnerA {}

    #[async_trait::async_trait]
    impl TaskConsumeFunc<TestA> for RunnerA {
        async fn consume(self: Arc<Self>, params: <TestA as TaskInfo>::Params) -> anyhow::Result<<TestA as TaskInfo>::Returns> {
            dbg!(&params);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_tasker() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let tasker = SingleTaskerConsumer::<TestA, RunnerA>::init(connection_str.as_str(), collection_name.as_str(), RunnerA {}).await;
        let tasker = Arc::new(tasker);
        let result = tasker.start().await;
        dbg!(&result);
    }

    #[tokio::test]
    async fn test_init() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let tasker = SingleTaskerConsumer::<TestA, RunnerA>::init(connection_str.as_str(), collection_name.as_str(), RunnerA {}).await;
        let tasker = Arc::new(tasker);
        tokio::spawn(tasker.start());
    }
}
