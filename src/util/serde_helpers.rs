use chrono::{DateTime, Local};
use serde::{Deserialize, Deserializer, ser, Serialize, Serializer};

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

pub fn serialize_datetime_option_as_datetime<S: Serializer>(
    val: &Option<DateTime<Local>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match val {
        None => serializer.serialize_none(),
        Some(duration) => {
            let datetime = mongodb::bson::DateTime::from_chrono(*duration);
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
    let datetime = mongodb::bson::DateTime::from_chrono(*val);
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