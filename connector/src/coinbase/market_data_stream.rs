use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
// use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, warn};

use crate::connector::PublishEvent;

pub struct MarketDataStream {
    // ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    // client: CoinbaseClient,
}

impl MarketDataStream {
    pub fn new(
        // ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        // client: CoinbaseClient,
    ) -> Self {
        Self {
            // ev_tx,
            symbol_rx,
        }
    }

    // TODO: decorate Error.
    pub async fn connect(
        &mut self,
        key_name: &str,
        key_secret: &str,
        url: &str,
    ) -> Result<(), Error> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        // TODO: handle jwt expire?
        let jwt = sign_es256(key_name, key_secret);
        loop {
            select! {
                // TODO: handle ping pong.
                // TODO: handle symbol subscription
                symbol_msg = self.symbol_rx.recv() => match symbol_msg{
                    Ok(symbol) => {
                        write.send(Message::Text(format!(r#"{{
                            "type": "subscribe",
                            "product_ids": ["{symbol}"],
                            "channel": "level2",
                            "jwt": "{jwt}"
                        }}"#).into() )).await?;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                // Handle websocket market data stream.
                ws_msg = read.next() => {
                    match ws_msg {
                        Some(Ok(msg)) => {
                            println!("{:?}", msg);
                        }
                        Some(Err(e)) => {
                            println!("Error: {:?}", e);
                        }
                        None => {
                            println!("WebSocket stream ended");
                            return Err(anyhow::anyhow!("WebSocket stream ended"));
                        }
                    }
                }
            }
        }
    }
}

// Defines the JWT claims
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iss: String,
    nbf: usize,
    exp: usize,
}

fn sign_es256(key_name: &str, key_secret: &str) -> String {
    // Build JWT header with ES256, kid
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_name.to_string());

    // Set issued-at and expiration (2 min) timestamps
    let iat = Utc::now().timestamp() as usize;
    let exp = iat + 120;

    // Create the claims
    let claims = Claims {
        sub: key_name.to_string(),
        iss: "cdp".to_string(),
        nbf: iat,
        exp,
    };

    // Encode the token
    let encoding_key = EncodingKey::from_ec_pem(key_secret.as_bytes()).unwrap();

    // Encode the token using ES256 and your EC key
    let token = encode(&header, &claims, &encoding_key).unwrap();

    return token;
}
