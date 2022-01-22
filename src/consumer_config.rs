use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsumerConfig {
    pub max_concurrency: u32,
    pub task_delay: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        ConsumerConfig {
            max_concurrency: u32::MAX,
            task_delay: Duration::ZERO,
        }
    }
}