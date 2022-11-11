use std::sync::atomic::AtomicUsize;

use mongodb::{Client, Collection};
use mongodb::options::{ClientOptions, ResolverConfig};
use tracing::info;

use crate::app::common::{TaskAppBasicOperations, TaskAppCommon};
use crate::app::consumer::TaskConsumeCore;
use crate::app::producer::TaskProducer;
use crate::task::{TaskConfig, TaskInfo, TaskRequest};

// Given a collection and consume method, handle all the rest work of TaskConsumer generation
pub struct SingleTaskerProducer<T: TaskInfo> {
    config: TaskConfig,
    concurrency: Option<AtomicUsize>,
    collection: Collection<TaskRequest<T>>,
}

impl<T: TaskInfo> TaskAppCommon<T> for SingleTaskerProducer<T> {
    fn get_collection(&self) -> &Collection<TaskRequest<T>> {
        &self.collection
    }
}

impl<T: TaskInfo> TaskAppBasicOperations<T> for SingleTaskerProducer<T> {}

impl<T: TaskInfo> TaskProducer<T> for SingleTaskerProducer<T> {}

#[async_trait::async_trait]
impl<T: TaskInfo> TaskConsumeCore<T> for SingleTaskerProducer<T> {
    fn get_default_option(&'_ self) -> &'_ TaskConfig {
        &self.config
    }

    fn get_concurrency(&'_ self) -> Option<&'_ AtomicUsize> {
        self.concurrency.as_ref()
    }
}

impl<T: TaskInfo> SingleTaskerProducer<T> {
    pub async fn init(connection_str: &str, collection_name: &str) -> Self {
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
        SingleTaskerProducer {
            config: Default::default(),
            concurrency: None,
            collection,
        }
    }
}

#[cfg(test)]
mod test {
    use std::env;
    use std::sync::Arc;

    use crate::app::common::TaskAppBasicOperations;
    use crate::task::TaskInfo;
    use crate::tasker::single_tasker_producer::SingleTaskerProducer;
    use crate::util::test_logger::tests::init_logger;

    struct TestA {}

    impl TaskInfo for TestA {
        type Params = ();
        type Returns = ();
    }

    #[tokio::test]
    async fn test_single_tasker() {
        init_logger();
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let tasker = SingleTaskerProducer::<TestA>::init(connection_str.as_str(), collection_name.as_str()).await;
        let tasker = Arc::new(tasker);
        let result = tasker.send_task("aaa", ()).await;
        dbg!(&result);
    }
}
