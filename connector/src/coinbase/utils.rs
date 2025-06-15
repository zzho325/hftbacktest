use chrono::{DateTime, Utc};
use futures_util::{SinkExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// Sends a WebSocket subscription message to a specified channel with optional product ID and JWT
/// authentication.
pub async fn subscribe_ws(
    ws_write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    channel: &str,
    product_id: Option<&str>,
    jwt: &str,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let mut msg = serde_json::json!({
        "type" : "subscribe",
        "channel" : channel,
        "jwt": jwt
    });
    if let Some(pid) = product_id {
        msg["product_ids"] = serde_json::json!([pid])
    }
    ws_write.send(Message::Text(msg.to_string().into())).await
}

/// A trait that abstracts access to the current UTC time so that time can me mocked for testing.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// The default system implementation of [`Clock`] that returns `Utc::now()`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(test)]
pub mod testutils {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, Utc};

    use crate::coinbase::utils::Clock;

    /// Mock implementation of [`Clock`] for testing.
    #[derive(Clone)]
    pub struct MockClock {
        time: Arc<Mutex<DateTime<Utc>>>,
    }

    impl MockClock {
        pub fn new(initial: DateTime<Utc>) -> Self {
            Self {
                time: Arc::new(Mutex::new(initial)),
            }
        }

        pub fn set_time(&self, new_time: DateTime<Utc>) {
            *self.time.lock().unwrap() = new_time;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> DateTime<Utc> {
            *self.time.lock().unwrap()
        }
    }
}

/// Defines the JWT claims
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iss: String,
    nbf: usize,
    exp: usize,
}

pub fn sign_es256(clock: &dyn Clock, key_name: &str, key_secret: &str) -> String {
    // Build JWT header with ES256, kid
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some(key_name.to_string());

    // Set issued-at and expiration (2 min) timestamps
    let iat = clock.now().timestamp() as usize;
    let exp = iat + 120;

    // Create the claims
    let claims = Claims {
        sub: key_name.to_string(),
        iss: "cdp".to_string(),
        nbf: iat,
        exp,
    };

    // Encode the token
    let encoding_key = jsonwebtoken::EncodingKey::from_ec_pem(key_secret.as_bytes()).unwrap();

    // Encode the token using ES256 and your EC key
    jsonwebtoken::encode(&header, &claims, &encoding_key).unwrap()
}
