use std::time::Instant;

use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, future::err};
use hftbacktest::{
    live::ipc::TO_ALL,
    types::{Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT, LiveEvent},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rand::rand_core::le;
// use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    coinbase::msg::stream::{Level2, Side, Stream},
    connector::PublishEvent,
};

pub struct MarketDataStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
}

impl MarketDataStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        // client: CoinbaseClient,
    ) -> Self {
        Self { ev_tx, symbol_rx }
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
        // TODO: send pong
        let mut last_ping = Instant::now();

        // TODO: subscribe to heartbeat.
        // TODO: handle jwt expire?
        let jwt = sign_es256(key_name, key_secret);
        loop {
            select! {
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
                ws_msg = read.next() => match ws_msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<Stream>(&text) {
                            Ok(Stream::Level2(level2)) => {
                                self.process_level2_channel(level2);
                            }
                            Ok(Stream::Subscriptions{events}) => {
                                for event in events {
                                    for (sub_key, values) in event.subscriptions {
                                        debug!(
                                            key = %sub_key,
                                            values = ?values,
                                            "Subscription request response is received",
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                error!(?error, %text, "Couldn't parse Stream.");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) =>  {
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        // TODO: handle error
                    }
                    Some( Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_)))
                    | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        // TODO: handle error
                    }
                    None => {
                        // TODO: handle error
                    }
                }
            }
        }
    }

    fn process_level2_channel(&mut self, data: Level2) {
        // TODO: track sequence number
        for event in data.events {
            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
            // TODO: handle snapshot and updates differently
            for update in event.updates {
                let depth_ev = match update.side {
                    Side::Bid => LOCAL_BID_DEPTH_EVENT,
                    Side::Ask => LOCAL_ASK_DEPTH_EVENT,
                };

                self.ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                        symbol: event.product_id.clone(),
                        event: Event {
                            ev: depth_ev,
                            exch_ts: update.event_time.timestamp_nanos_opt().unwrap(),
                            local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                            order_id: 0,
                            px: update.price_level,
                            qty: update.new_quantity,
                            ival: 0,
                            fval: 0.0,
                        },
                    }))
                    .unwrap();
            }
            self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
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
