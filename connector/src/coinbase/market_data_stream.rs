use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::{TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{
    live::ipc::TO_ALL,
    types::{Event, LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT, LiveEvent},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    coinbase::{
        CoinbaseError,
        msg::stream::{ChannelMessage, Level2, Side, Stream},
    },
    connector::PublishEvent,
};

pub struct MarketDataStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    subscription_tracker: SubscriptionTracker,
    /// Tracks if snapshot has been received for a symbol from level2 channel.
    level2_symbol_status: HashMap<String, bool>,
}

impl MarketDataStream {
    // Coinbase sends heartbeats per second, errors out when no heartbeat is received for 2 sec.
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(ev_tx: UnboundedSender<PublishEvent>, symbol_rx: Receiver<String>) -> Self {
        Self {
            ev_tx,
            symbol_rx,
            subscription_tracker: SubscriptionTracker::new(),
            level2_symbol_status: Default::default(),
        }
    }

    pub async fn connect(
        &mut self,
        key_name: &str,
        key_secret: &str,
        url: &str,
    ) -> Result<(), CoinbaseError> {
        let request = url.into_client_request().map_err(Box::new)?;
        let (ws_stream, _) = connect_async(request).await.map_err(Box::new)?;
        let (mut write, mut read) = ws_stream.split();

        // TODO: handle jwt expire?
        let jwt = sign_es256(key_name, key_secret);
        // Subscribe to heartbeat.
        write
            .send(Message::Text(
                format!(
                    r#"{{
                            "type": "subscribe",
                            "channel": "heartbeats",
                            "jwt": "{jwt}"
                        }}"#
                )
                .into(),
            ))
            .await
            .map_err(Box::new)?;

        let mut heartbeat_interval = time::interval(Self::HEARTBEAT_INTERVAL);
        heartbeat_interval.tick().await; // skip first tick
        let mut last_heartbeat: Option<Instant> = None;

        loop {
            select! {
                _ = heartbeat_interval.tick()  => {
                    if last_heartbeat
                        .map(|t| t.elapsed() > Self::HEARTBEAT_INTERVAL)
                        .unwrap_or(true) {
                            return Err(CoinbaseError::ConnectionInterrupted(
                                "missing heartbeats".to_string()
                            ));
                    }
                },
                symbol_msg = self.symbol_rx.recv() => {
                    match symbol_msg {
                        Ok(symbol) => {
                            write.send(Message::Text(
                                format!(
                                    r#"{{
                                        "type": "subscribe",
                                        "product_ids": ["{symbol}"],
                                        "channel": "level2",
                                        "jwt": "{jwt}"
                                    }}"#
                                )
                                .into(),
                            ))
                            .await
                            .map_err(Box::new)?;
                        },
                        Err(RecvError::Closed) => {
                            return Ok(());
                        },
                        Err(RecvError::Lagged(num)) => {
                            return Err(CoinbaseError::SubscriptionRequestMissed(
                                format!("{num} Subscription requests were missed.")
                            ));
                        },
                    }
                },
                ws_msg = read.next() => {
                    match ws_msg {
                        Some(Ok(Message::Text(text))) => {
                            self.process_text_messages(text.to_string(), &mut last_heartbeat)?;
                        },
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await.map_err(Box::new)?;
                        },
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(CoinbaseError::ConnectionAbort(
                                format!("Websocket closed: {close_frame:?}")
                           ));
                        },
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {},
                        Some(Err(error)) => {
                            return Err(CoinbaseError::from(Box::new(error)));
                        },
                        None => {
                            return Err(CoinbaseError::ConnectionInterrupted(
                                "No websocket message".to_string()
                            ));
                        },
                    }
                },
            }
        }
    }

    fn process_text_messages(
        &mut self,
        text: String,
        last_heartbeat: &mut Option<Instant>,
    ) -> Result<(), CoinbaseError> {
        match serde_json::from_str::<Stream>(&text) {
            Ok(Stream::Subscriptions { events }) => {
                for event in events {
                    for (sub_key, values) in event.subscriptions {
                        debug!(
                            key = %sub_key,
                            values = ?values,
                            "Subscription request response is received",
                        );
                    }
                }
                Ok(())
            }
            Ok(Stream::Heartbeat(heartbeat)) => {
                self.subscription_tracker
                    .track_seq("heartbeat", heartbeat.sequence_num);
                if heartbeat.events.len() != 1 {
                    return Err(CoinbaseError::WebSocketStreamError(format!(
                        "Invalid heartbeat {heartbeat:?}"
                    )));
                }
                let event = &heartbeat.events[0];
                self.subscription_tracker
                    .track_heartbeat_counter(event.heartbeat_counter);
                *last_heartbeat = Some(Instant::now());
                // Error out if heartbeat timestamp > 2 seconds behind.
                let now_utc = chrono::Utc::now();
                let age = now_utc.signed_duration_since(event.current_time);
                if age > TimeDelta::from_std(Self::HEARTBEAT_INTERVAL).unwrap() {
                    // TODO: error out after extracting clock.
                    // return Err(CoinbaseError::WebSocketStreamError(format!(
                    //     "Heartbeat is {} seconds old!",
                    //     age.num_seconds()
                    // )));
                }
                Ok(())
            }
            Ok(Stream::Level2(level2)) => self.process_level2_channel(level2),
            // TODO: handle authentication failure
            // {"type":"error","message":"authentication failure"}
            Err(error) => Err(CoinbaseError::WebSocketStreamError(format!(
                "Couldn't parse Stream {text}: {error}"
            ))),
        }
    }

    fn process_level2_channel(
        &mut self,
        data: ChannelMessage<Level2>,
    ) -> Result<(), CoinbaseError> {
        self.subscription_tracker
            .track_seq("level2", data.sequence_num);

        for event in data.events {
            let symbol = &event.product_id;
            let snapshots = &mut self.level2_symbol_status;
            if snapshots.get(symbol).copied().unwrap_or(false) {
                if event.type_ == "snapshot" {
                    // TODO: a second snapshot should clean depth
                    error!(channel = "level2", "second snapshot received");
                }
            } else if event.type_ == "snapshot" {
                snapshots.insert(symbol.clone(), true);
            } else {
                debug!(channel = "level2", "snapshot not received, ignore update",);
                continue;
            }

            // TODO: when connector is killed sending messages here would panic because receivers
            // has been dropped, should refactor connector exit order
            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
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
        Ok(())
    }
}

/// Tracks missing messages for a websocket subscription with sequence numbers and heartbeat
/// counter, logging any out-of-order or missing (dropped) messages.
struct SubscriptionTracker {
    prev_seq: Option<u64>,
    prev_heartbeat_counter: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SequenceStatus {
    InOrder,
    OutOfOrder { expected: u64, actual: u64 },
    GapDetected { expected: u64, actual: u64 },
}

impl SubscriptionTracker {
    fn new() -> Self {
        SubscriptionTracker {
            prev_seq: None,
            prev_heartbeat_counter: None,
        }
    }

    /// Feed in the message's channel and its sequence number, logs out-of-order or dropped
    /// messages.
    fn track_seq(&mut self, channel: &str, seq: u64) {
        let status = Self::track(&mut self.prev_seq, seq);
        if status != SequenceStatus::InOrder {
            // TODO: figure out how to handle
            debug!("{channel} message not in sequence: {status:?}");
        }
    }

    /// Feed in the heartbeat counter. logs out-of-order or dropped heartbeats.
    fn track_heartbeat_counter(&mut self, counter: u64) {
        let status = Self::track(&mut self.prev_heartbeat_counter, counter);
        if status != SequenceStatus::InOrder {
            // TODO: figure out how to handle
            debug!("heartbeat counter not in sequence: {status:?}");
        }
    }

    fn track(prev: &mut Option<u64>, cur: u64) -> SequenceStatus {
        let expected = prev.map(|p| p + 1);
        *prev = Some(cur);
        match expected {
            Some(expected) if cur < expected => SequenceStatus::OutOfOrder {
                expected,
                actual: cur,
            },
            Some(expected) if cur > expected => SequenceStatus::GapDetected {
                expected,
                actual: cur,
            },
            _ => SequenceStatus::InOrder,
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
    encode(&header, &claims, &encoding_key).unwrap()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use chrono::DateTime;
    use hftbacktest::{
        live::ipc::TO_ALL,
        types::{LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT, LiveEvent},
    };
    use serde_json::json;
    use tokio::sync::{broadcast, mpsc};

    use crate::{coinbase::market_data_stream::MarketDataStream, connector::PublishEvent};

    #[tokio::test]
    async fn process_level2_messages() {
        let (tx, mut rx) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let mut stream = MarketDataStream::new(tx, sym_rx);
        let mut last_heartbeat: Option<Instant> = None;

        let test_ts = "2025-06-01T20:32:50Z";
        let test_ts_nano = DateTime::parse_from_rfc3339(test_ts)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();

        let snapshot = json!({
          "channel": "l2_data",
          "client_id": "",
          "timestamp": "2025-06-01T20:32:50.714964855Z",
          "sequence_num": 2,
          "events": [
            {
              "type": "snapshot",
              "product_id": "BTC-USD",
              "updates": [
                {
                  "side": "bid",
                  "event_time": test_ts,
                  "price_level": "21921.73",
                  "new_quantity": "0.06317902"
                },
                {
                  "side": "offer",
                  "event_time": test_ts,
                  "price_level": "21921.3",
                  "new_quantity": "0.024"
                }
              ]
            }
          ]
        })
        .to_string();

        let update = json!({
          "channel": "l2_data",
          "client_id": "",
          "timestamp": "2025-06-01T20:32:50.714964855Z",
          "sequence_num": 1,
          "events": [
            {
              "type": "update",
              "product_id": "BTC-USD",
              "updates": [
                {
                  "side": "bid",
                  "event_time": test_ts,
                  "price_level": "21921.73",
                  "new_quantity": "0.06317902"
                },
                {
                  "side": "offer",
                  "event_time": test_ts,
                  "price_level": "21921.3",
                  "new_quantity": "0.024"
                }
              ]
            }
          ]
        })
        .to_string();

        // Verify update before snapshot is ignored.
        stream
            .process_text_messages(update, &mut last_heartbeat)
            .expect("expect no error");

        assert!(
            rx.try_recv().is_err(),
            "Expect no PublishEvent for update before snapshot"
        );
        assert_eq!(
            Some(1),
            stream.subscription_tracker.prev_seq,
            "expect sequence number tracked"
        );

        // Verify snapshot is processed.
        stream
            .process_text_messages(snapshot, &mut last_heartbeat)
            .expect("expect no error");

        match rx.try_recv().unwrap() {
            PublishEvent::BatchStart(dest) => assert_eq!(dest, TO_ALL),
            other => panic!("expect BatchStart, got {other:?}"),
        }
        match rx.try_recv().unwrap() {
            PublishEvent::LiveEvent(LiveEvent::Feed { event, .. }) => {
                assert_eq!(event.ev, LOCAL_BID_DEPTH_EVENT);
                assert_eq!(event.exch_ts, test_ts_nano);
                assert_eq!(event.px, 21921.73);
                assert_eq!(event.qty, 0.06317902);
            }
            other => panic!("expect LiveEvent, got {other:?}"),
        }
        match rx.try_recv().unwrap() {
            PublishEvent::LiveEvent(LiveEvent::Feed { event, .. }) => {
                assert_eq!(event.ev, LOCAL_ASK_DEPTH_EVENT);
                assert_eq!(event.exch_ts, test_ts_nano);
                assert_eq!(event.px, 21921.3);
                assert_eq!(event.qty, 0.024);
            }
            other => panic!("expect LiveEvent, got {other:?}"),
        }
        match rx.try_recv().unwrap() {
            PublishEvent::BatchEnd(dest) => assert_eq!(dest, TO_ALL),
            other => panic!("expect BatchEnd, got {other:?}"),
        }
        assert_eq!(
            Some(2),
            stream.subscription_tracker.prev_seq,
            "expect sequence number tracked"
        );
    }

    #[tokio::test]
    async fn process_heartbeats() {
        let (tx, _) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let mut stream = MarketDataStream::new(tx, sym_rx);
        let mut last_heartbeat: Option<Instant> = None;

        let heartbeat_ts = "2025-06-01 20:32:50.121961769 +0000 UTC";
        let heartbeat = json!({
          "channel": "heartbeats",
          "client_id": "",
          "timestamp": "2023-06-23T20:31:26.122969572Z",
          "sequence_num": 1,
          "events": [
            {
              "current_time": format!("{heartbeat_ts} m=+91717.525857105"),
              "heartbeat_counter": 3049
            }
          ]
        })
        .to_string();

        stream
            .process_text_messages(heartbeat, &mut last_heartbeat)
            .expect("expect no error");

        assert_eq!(
            Some(1),
            stream.subscription_tracker.prev_seq,
            "expect sequence number tracked"
        );
        assert_eq!(
            Some(3049),
            stream.subscription_tracker.prev_heartbeat_counter,
            "expect heartbeat counter tracked"
        );
    }
}
