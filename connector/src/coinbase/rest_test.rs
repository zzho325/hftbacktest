//! Integration tests calling private functions to exercise REST client.
//! TODO: for Coinbase APIs with no sandbox support (WebSocket and future related endpoints),
//! implement mock service for testing. Similar tests exercising connector with public interface
//! (through IPC) should be added to crate level integration tests.

use std::fs::read_to_string;

use serde_json::json;

use crate::coinbase::{Config, msg::Side, rest::CoinbaseClient, utils::JwtSigner};

const SANDBOX_URL: &str = "https://api-sandbox.coinbase.com/api/v3/brokerage";

/// Creating order sanity check.
#[tokio::test]
#[ignore]
async fn create_order_sanbox() {
    let path = std::env::var("CONFIG").expect("Missing CONFIG env var");
    let config_str = read_to_string(path).unwrap();
    let config: Config = toml::from_str(&config_str).unwrap();

    let client = CoinbaseClient::new(
        JwtSigner::new(config.api_key_name, config.api_key_secret),
        SANDBOX_URL,
    );
    let res = client
        .submit_order(
            "dummy_order_id",
            "BTC-USD",
            Side::Sell,
            json!({
                "market_market_ioc": {
                    "quote_size":"10",
                    "rfq_disabled":false
                }
            }),
        )
        .await;
    let order_resp = res.expect("Order submission failed");
    assert!(order_resp.success, "expect order success")
}
