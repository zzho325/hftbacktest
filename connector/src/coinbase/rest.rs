use async_trait::async_trait;
use reqwest::Url;
use serde::{Serialize, de::DeserializeOwned};

use crate::coinbase::{
    RestClientError,
    msg::rest::{OrderRequest, OrderResponse},
    utils::{self},
};

type Result<T> = std::result::Result<T, RestClientError>;

#[async_trait]
pub trait CoinbaseClient: Clone + Send + Sync + 'static {
    async fn submit_order(&self, order_req: OrderRequest) -> Result<OrderResponse>;
}

#[derive(Clone)]
pub struct CoinbaseClientImpl {
    jwt_signer: utils::JwtSigner,
    rest_api_url: String,
    client: reqwest::Client,
}

#[async_trait]
impl CoinbaseClient for CoinbaseClientImpl {
    async fn submit_order(&self, order_req: OrderRequest) -> Result<OrderResponse> {
        self.post::<OrderRequest, OrderResponse>("orders", &order_req)
            .await
    }
}

impl CoinbaseClientImpl {
    pub fn new(jwt_signer: utils::JwtSigner, host: impl Into<String>) -> Self {
        Self {
            jwt_signer,
            rest_api_url: host.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn get<T: DeserializeOwned>(&self, resource: &str) -> Result<T> {
        let parsed = Url::parse(&self.rest_api_url).map_err(|e| {
            RestClientError::InternalError(format!("error parsing REST API url {e}"))
        })?;
        let request_host = parsed.host_str().ok_or_else(|| {
            RestClientError::InternalError(format!("REST URL has no host: {}", self.rest_api_url))
        })?;
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

    async fn post<U: Serialize, T: DeserializeOwned>(
        &self,
        resource: &str,
        payload: &U,
    ) -> Result<T> {
        let parsed = Url::parse(&self.rest_api_url).map_err(|e| {
            RestClientError::InternalError(format!("error parsing REST API url {e}"))
        })?;
        let request_host = parsed.host_str().ok_or_else(|| {
            RestClientError::InternalError(format!("REST URL has no host: {}", self.rest_api_url))
        })?;
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
}
