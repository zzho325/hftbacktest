use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};

use crate::utils::from_str_to_f64;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Stream {
    Channel(Channel),
    Error(Error),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "channel")]
pub enum Channel {
    #[serde(rename = "subscriptions")]
    Subscriptions { events: Vec<SubscriptionEvent> },
    #[serde(rename = "l2_data")]
    Level2(ChannelMessage<Level2>),
    #[serde(rename = "market_trades")]
    Trade(ChannelMessage<Trade>),
    #[serde(rename = "heartbeats")]
    Heartbeat(ChannelMessage<Heartbeat>),
}

#[derive(Deserialize, Debug)]
pub struct SubscriptionEvent {
    pub subscriptions: std::collections::HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct ChannelMessage<T> {
    // pub client_id: String,
    // pub timestamp: DateTime<Utc>,
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

#[derive(Deserialize, Debug)]
pub enum BookSide {
    #[serde(rename = "bid")]
    Bid,
    #[serde(rename = "offer")]
    Ask,
}

#[derive(Deserialize, Debug)]
pub struct L2Update {
    pub side: BookSide,
    pub event_time: DateTime<Utc>,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price_level: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub new_quantity: f64,
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    // #[serde(rename = "type")]
    // pub type_: String,
    pub trades: Vec<TradeEvent>,
}

#[derive(Deserialize, Debug)]
pub struct TradeEvent {
    // pub trade_id: u64,
    pub product_id: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub size: f64,
    pub side: Side,
    pub time: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
pub enum Side {
    #[serde(rename = "SELL")]
    Sell,
    #[serde(rename = "BUY")]
    Buy,
}

#[derive(Deserialize, Debug)]
pub struct Heartbeat {
    #[serde(deserialize_with = "from_heartbeat_ts_to_datetime")]
    pub current_time: DateTime<Utc>,
    pub heartbeat_counter: u64,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Error {
    #[serde(rename = "error")]
    ErrorMessage { message: String },
}

/// Deserialize heartbeat current_time of format
/// "2025-06-06 07:01:53.305743326 +0000 UTC m=+27862.572888510".
fn from_heartbeat_ts_to_datetime<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
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
