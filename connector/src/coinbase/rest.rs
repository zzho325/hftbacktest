use reqwest::Url;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;

use crate::coinbase::{
    RestClientError,
    msg::{Side, rest::OrderResponse},
    utils::{self, Clock, SystemClock},
};

pub struct CoinbaseClient {
    jwt_signer: utils::JwtSigner,
    utc_clock: Box<dyn Clock>,
    rest_api_url: String,
    client: reqwest::Client,
}

type Result<T> = std::result::Result<T, RestClientError>;

impl CoinbaseClient {
    pub fn new(jwt_signer: utils::JwtSigner, host: impl Into<String>) -> Self {
        Self {
            jwt_signer,
            utc_clock: Box::new(SystemClock),
            rest_api_url: host.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn get<T: DeserializeOwned>(&self, resource: &str) -> Result<T> {
        let parsed = Url::parse(&self.rest_api_url).unwrap();
        let request_host = parsed.host_str().unwrap();
        let base_path = parsed.path();
        let uri = format!("GET {request_host}{base_path}/{resource}");
        let jwt = self.jwt_signer.sign_with_uri(uri);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {jwt}").parse().map_err(|e| {
                RestClientError::InternalError(format!("Error constructing hearder {e}",))
            })?,
        );

        let resp = self
            .client
            .get(format!("{}/{}", self.rest_api_url, resource))
            .headers(headers)
            .send()
            .await?;
        Self::handle_resp(resp).await
    }

    async fn post<T: DeserializeOwned, U: Serialize>(
        &self,
        resource: &str,
        payload: &U,
    ) -> Result<T> {
        let parsed = Url::parse(&self.rest_api_url).unwrap();
        let request_host = parsed.host_str().unwrap();
        let base_path = parsed.path();

        let uri = format!("POST {request_host}{base_path}/{resource}");

        let jwt = self.jwt_signer.sign_with_uri(uri);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {jwt}").parse().map_err(|e| {
                RestClientError::InternalError(format!("Error constructing hearder {e}",))
            })?,
        );

        let resp = self
            .client
            .post(format!("{}/{}", self.rest_api_url, resource))
            .json(payload)
            .headers(headers)
            .send()
            .await?;

        Self::handle_resp(resp).await
    }

    async fn handle_resp<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| RestClientError::ServiceError {
                status,
                body: format!("Failed to read body: {e}"),
            })?;

        if !status.is_success() {
            return Err(RestClientError::ServiceError { status, body });
        }

        let result: T = serde_json::from_str(&body).map_err(|e| RestClientError::ServiceError {
            status,
            body: format!("Could not deserialize resp {body}: {e}"),
        })?;

        Ok(result)
    }

    // TODO: using json value for order_configuration for now, should switch to typed struct.
    pub async fn submit_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        order_configuration: serde_json::Value,
    ) -> Result<OrderResponse> {
        let payload = json!({
            "client_order_id": client_order_id,
            "product_id": symbol,
            "side": side,
            "order_configuration": order_configuration
        });

        let resp = self
            .post::<OrderResponse, serde_json::Value>("orders", &payload)
            .await?;
        Ok(resp)
    }
}
