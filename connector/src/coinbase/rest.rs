use reqwest::Url;

use crate::coinbase::utils::{self, Clock, SystemClock};

pub struct CoinbaseClient {
    jwt_signer: utils::JwtSigner,
    utc_clock: Box<dyn Clock>,
    rest_api_url: String,
    client: reqwest::Client,
}

impl CoinbaseClient {
    pub fn new(jwt_signer: utils::JwtSigner, host: impl Into<String>) -> Self {
        Self {
            jwt_signer,
            utc_clock: Box::new(SystemClock),
            rest_api_url: host.into(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn get(&self, resource: &str) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::builder().build()?;

        let parsed = Url::parse(&self.rest_api_url).unwrap();
        let request_host = parsed.host_str().unwrap(); // "api.coinbase.com"
        let base_path = parsed.path();

        let uri = format!("GET {request_host}{base_path}/{resource}");

        let jwt = self.jwt_signer.sign_with_uri(uri);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Authorization", format!("Bearer {jwt}").parse()?);
        headers.insert("Content-Type", "application/json".parse()?);

        let request = client
            .request(
                reqwest::Method::GET,
                format!("{}/{}", self.rest_api_url, resource),
            )
            .headers(headers);

        let response = request.send().await?;
        let body = response.text().await?;

        println!("{body}");

        Ok(())
    }
}
