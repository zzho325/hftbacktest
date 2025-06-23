use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::coinbase::msg::Side;

#[derive(Serialize, Debug)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub product_id: String,
    pub side: Side,
    #[serde(rename = "order_configuration")]
    pub order_config: OrderConfiguration,
}

#[derive(Serialize, Debug)]
pub enum OrderConfiguration {
    #[serde(rename = "market_market_ioc")]
    MarketIOC { quote_size: String },
    #[serde(rename = "sor_limit_ioc")]
    LimitIOC {
        quote_size: String,
        limit_price: String,
    },
    #[serde(rename = "limit_limit_gtc")]
    LimitGTC {
        quote_size: String,
        limit_price: String,
        post_only: bool,
    },
    #[serde(rename = "limit_limit_fok")]
    LimitFOK {
        quote_size: String,
        limit_price: String,
    },
}

#[derive(Deserialize, Debug)]
pub struct OrderResponse {
    pub success: bool,
    pub success_response: Option<SuccessResponse>,
    pub error_response: Option<ErrorResponse>,
    pub order_configuration: Value,
}

#[derive(Deserialize, Debug)]
pub struct SuccessResponse {
    pub order_id: String,
    pub product_id: String,
    pub side: Side,
    pub client_order_id: String,
    pub attached_order_id: String,
}

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    pub error_details: String,
    pub preview_failure_reason: String,
    pub new_order_failure_reason: String,
}
