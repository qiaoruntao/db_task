use std::convert::TryFrom;
use std::fmt::Debug;
use std::time::Duration;

use chrono::{DateTime, Local};
use mongodb::bson::doc;
use mongodb::bson::Document;
use serde::{Deserialize, Deserializer, ser, Serialize, Serializer};

pub type DefaultTaskState = Option<()>;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Task<ParamType, StateType> {
    // for deduplicate purpose
    pub key: String,
    // metadata
    pub meta: TaskMeta,
    // for task schedule
    pub option: TaskOptions,
    // task schedule data
    pub task_state: TaskState,
    // custom task param
    pub param: ParamType,
    // custom task state
    pub state: Option<StateType>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskState {
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub start_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub ping_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub next_ping_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub next_retry_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub complete_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub cancel_time: Option<DateTime<Local>>,
    #[serde(
    serialize_with = "serialize_datetime_option_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime_option"
    )]
    pub previous_fail_time: Option<DateTime<Local>>,
    pub current_worker_id: Option<i64>,
    pub progress: Option<u32>,
    // how many times can we retry
    pub retry_time_left: Option<u32>,
}

impl From<&TaskOptions> for TaskState {
    fn from(task_options: &TaskOptions) -> Self {
        TaskState {
            start_time: None,
            ping_time: None,
            next_ping_time: None,
            next_retry_time: None,
            complete_time: None,
            previous_fail_time: None,
            cancel_time: None,
            current_worker_id: None,
            progress: None,
            retry_time_left: Some(task_options.max_retries),
        }
    }
}

impl<
    'de,
    ParamType: Debug + Serialize + Deserialize<'de>,
    StateType: Debug + Serialize + Deserialize<'de>,
> Task<ParamType, StateType>
{
    pub fn generate_key_doc(&self) -> Document {
        doc! {
            "key":&self.key
        }
    }
}

pub fn serialize_u32_as_i32<S: Serializer>(val: &u32, serializer: S) -> Result<S::Ok, S::Error> {
    match i32::try_from(*val) {
        Ok(val) => serializer.serialize_i32(val),
        Err(_) => Err(ser::Error::custom(format!("cannot convert {} to i32", val))),
    }
}

pub fn deserialize_i32_as_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: Deserializer<'de>,
{
    let f = i32::deserialize(deserializer)?;
    Ok(f as u32)
}

pub fn serialize_duration_as_i64<S: Serializer>(
    val: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_i64(val.as_millis() as i64)
}

pub fn deserialize_i64_as_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
{
    let ms_val = i64::deserialize(deserializer)?;
    Ok(Duration::from_millis(ms_val as u64))
}

pub fn serialize_duration_option_as_i64<S: Serializer>(
    val: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match val {
        None => serializer.serialize_none(),
        Some(duration) => serializer.serialize_i64(duration.as_millis() as i64),
    }
}

pub fn deserialize_i64_as_duration_option<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
{
    let ms_val = Option::<i64>::deserialize(deserializer)?;
    match ms_val {
        None => Ok(None),
        Some(ms) => Ok(Some(Duration::from_millis(ms as u64))),
    }
}

pub fn serialize_datetime_option_as_datetime<S: Serializer>(
    val: &Option<DateTime<Local>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match val {
        None => serializer.serialize_none(),
        Some(duration) => {
            let datetime = mongodb::bson::DateTime::from_chrono(duration.clone());
            datetime.serialize(serializer)
        }
    }
}

pub fn deserialize_datetime_as_datetime_option<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Local>>, D::Error>
    where
        D: Deserializer<'de>,
{
    let ms_val = Option::<mongodb::bson::DateTime>::deserialize(deserializer)?;
    match ms_val {
        None => Ok(None),
        Some(datetime) => {
            let time = datetime.to_chrono();
            Ok(Option::from(DateTime::<Local>::from(time)))
        }
    }
}

pub fn serialize_datetime_as_datetime<S: Serializer>(
    val: &DateTime<Local>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let datetime = mongodb::bson::DateTime::from_chrono(val.clone());
    datetime.serialize(serializer)
}

pub fn deserialize_datetime_as_datetime<'de, D>(
    deserializer: D,
) -> Result<DateTime<Local>, D::Error>
    where
        D: Deserializer<'de>,
{
    let datetime = mongodb::bson::DateTime::deserialize(deserializer)?;
    let time = datetime.to_chrono();
    Ok(DateTime::<Local>::from(time))
}

#[derive(Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct TaskOptions {
    #[serde(
    serialize_with = "serialize_duration_option_as_i64",
    deserialize_with = "deserialize_i64_as_duration_option"
    )]
    pub time_limit: Option<Duration>,
    #[serde(
    serialize_with = "serialize_u32_as_i32",
    deserialize_with = "deserialize_i32_as_u32"
    )]
    pub max_retries: u32,
    #[serde(
    serialize_with = "serialize_duration_as_i64",
    deserialize_with = "deserialize_i64_as_duration"
    )]
    pub min_retry_delay: Duration,
    #[serde(
    serialize_with = "serialize_duration_as_i64",
    deserialize_with = "deserialize_i64_as_duration"
    )]
    pub max_retry_delay: Duration,
    // how often should we update ping time
    #[serde(
    serialize_with = "serialize_duration_as_i64",
    deserialize_with = "deserialize_i64_as_duration"
    )]
    pub ping_interval: Duration,
    // should we retry after task throw error
    pub retry_for_unexpected: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskMeta {
    // human friendly task name
    pub name: String,
    // when did the task create
    #[serde(
    serialize_with = "serialize_datetime_as_datetime",
    deserialize_with = "deserialize_datetime_as_datetime"
    )]
    pub create_time: DateTime<Local>,
    // who create this task
    pub creator: String,
}

impl TaskMeta {
    pub fn create_now(name: String, creator: String) -> Self {
        TaskMeta {
            name,
            create_time: Local::now(),
            creator,
        }
    }
}

impl Default for TaskOptions {
    fn default() -> Self {
        TaskOptions {
            time_limit: None,
            max_retries: 5,
            min_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            ping_interval: Duration::from_secs(30),
            retry_for_unexpected: true,
        }
    }
}

#[cfg(test)]
mod test_task {
    #[test]
    fn send_task() {}
}
