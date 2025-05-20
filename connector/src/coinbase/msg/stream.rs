use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::utils::from_str_to_f64;

#[derive(Deserialize, Debug)]
#[serde(tag = "channel")]
pub enum Stream {
    #[serde(rename = "subscriptions")]
    Subscriptions { events: Vec<SubscriptionEvent> },
    #[serde(rename = "l2_data")]
    Level2(Level2),
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionEvent {
    pub subscriptions: std::collections::HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct Level2 {
    pub client_id: String,
    pub timestamp: DateTime<Utc>,
    pub sequence_num: u64,
    pub events: Vec<Event>,
}

#[derive(Deserialize, Debug)]
pub struct Event {
    #[serde(rename = "type")]
    pub type_: String,
    pub product_id: String,
    pub updates: Vec<Update>,
}

#[derive(Debug, Deserialize)]
pub enum Side {
    #[serde(rename = "bid")]
    Bid,
    #[serde(rename = "offer")]
    Ask,
}

#[derive(Deserialize, Debug)]
pub struct Update {
    pub side: Side,
    pub event_time: DateTime<Utc>,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price_level: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub new_quantity: f64,
}
