use chrono::Duration;
use serde_with::serde::{Deserialize, Serialize};

#[serde_with::serde_as]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskOptions {
    #[serde_as(as = "Option<serde_with::DurationSeconds<i64>>")]
    pub time_limit: Option<Duration>,
    #[serde_as(as = "Option<serde_with::DurationSeconds<i64>>")]
    pub ping: Option<Duration>,
    pub max_retries: Option<u32>,
    #[serde_as(as = "Option<serde_with::DurationSeconds<i64>>")]
    pub min_retry_delay: Option<Duration>,
    #[serde_as(as = "Option<serde_with::DurationSeconds<i64>>")]
    pub max_retry_delay: Option<Duration>,
    pub retry_for_unexpected: Option<bool>,
}

impl TaskOptions {
    pub fn get_ping_time(&self) -> Duration {
        self.ping.unwrap_or_else(||Duration::seconds(10))
    }
}

impl Default for TaskOptions {
    fn default() -> TaskOptions {
        TaskOptions {
            time_limit: None,
            ping: Some(Duration::seconds(30)),
            max_retries: None,
            min_retry_delay: None,
            max_retry_delay: None,
            retry_for_unexpected: None,
        }
    }
}