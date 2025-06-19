use serde::{Deserialize, Serialize};

pub mod rest;
pub mod stream;

#[derive(Deserialize, Debug, Serialize)]
pub enum Side {
    #[serde(rename = "SELL")]
    Sell,
    #[serde(rename = "BUY")]
    Buy,
}
