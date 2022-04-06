use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::util::serde_helpers::*;

// #[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskState {
    /// task creation time
    #[serde(
    serialize_with = "serialize_datetime_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime"
    )]
    // #[serde_as(as = "Option<serde_with::DurationSeconds<i64>>")]
    pub create_time: DateTime<Local>,
    /// next time can this task run, None means run immediately
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub next_run_time: Option<DateTime<Local>>,
    /// success time
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub success_time: Option<DateTime<Local>>,
    /// latest heartbeat time when task is running
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub ping_time: Option<DateTime<Local>>,
    /// when the task is canceled
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub cancel_time: Option<DateTime<Local>>,
    /// latest fail time
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub prev_fail_time: Option<DateTime<Local>>,
}
// #[serde_with::serde_as]

#[serde_with::skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskStateNextRunTime {
    /// next time can this task run
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    #[serde(default)]
    pub next_run_time: Option<DateTime<Local>>,
}

impl TaskState {
    pub fn can_run(&self) -> bool {
        if let Some(next_run_time) = self.next_run_time {
            next_run_time <= chrono::Local::now()
        } else {
            // if not specified, run immediately
            warn!("next_run_time not set");
            true
        }
    }

    /// current time related
    pub fn gen_initial(allow_run_time: Option<DateTime<Local>>) -> TaskState {
        let now = Local::now();
        TaskState {
            create_time: now,
            next_run_time: allow_run_time,
            success_time: None,
            ping_time: None,
            cancel_time: None,
            prev_fail_time: None,
        }
    }

    pub fn not_completed(&self) -> bool {
        self.cancel_time.is_none()
            && self.success_time.is_none()
    }
}