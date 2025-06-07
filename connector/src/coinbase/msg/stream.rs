use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, de::Error as DeError};

use crate::utils::from_str_to_f64;

#[derive(Deserialize, Debug)]
#[serde(tag = "channel")]
pub enum Stream {
    #[serde(rename = "subscriptions")]
    Subscriptions { events: Vec<SubscriptionEvent> },
    #[serde(rename = "l2_data")]
    Level2(ChannelMessage<Level2>),
    #[serde(rename = "heartbeats")]
    Heartbeat(ChannelMessage<Heartbeat>),
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionEvent {
    pub subscriptions: std::collections::HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct ChannelMessage<T> {
    pub client_id: String,
    pub timestamp: DateTime<Utc>,
    pub sequence_num: u64,
    pub events: Vec<T>,
}

#[derive(Deserialize, Debug)]
pub struct Level2 {
    #[serde(rename = "type")]
    pub type_: String,
    pub product_id: String,
    pub updates: Vec<L2Update>,
}

#[derive(Debug, Deserialize)]
pub enum Side {
    #[serde(rename = "bid")]
    Bid,
    #[serde(rename = "offer")]
    Ask,
}

#[derive(Deserialize, Debug)]
pub struct L2Update {
    pub side: Side,
    pub event_time: DateTime<Utc>,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price_level: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub new_quantity: f64,
}

#[derive(Deserialize, Debug)]
pub struct Heartbeat {
    #[serde(deserialize_with = "deserialize_heartbeat_timestamp")]
    pub current_time: DateTime<Utc>,
    pub heartbeat_counter: u64,
}

/// Deserialize heartbeat current_time of format
/// "2025-06-06 07:01:53.305743326 +0000 UTC m=+27862.572888510".
fn deserialize_heartbeat_timestamp<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    let trimmed = s.split(" m=").next().ok_or_else(|| {
        <D::Error as serde::de::Error>::custom("invalid Go timestamp: missing ` m=` delimiter")
    })?;
    DateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f %z UTC")
        .map_err(serde::de::Error::custom)
        .map(|dt| dt.with_timezone(&Utc))
}
