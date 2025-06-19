use serde::Deserialize;
use serde_json::Value;

use crate::coinbase::msg::Side;

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
