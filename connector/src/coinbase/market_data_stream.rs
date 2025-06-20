use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use hftbacktest::{
    live::ipc::TO_ALL,
    types::{
        Event,
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT,
        LOCAL_SELL_TRADE_EVENT,
        LiveEvent,
    },
};
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
        MarketStreamError,
        msg::{
            Side,
            stream::{BookSide, Channel, ChannelMessage, Error, Level2, Stream, Trade},
        },
        utils::{self, Clock},
    },
    connector::PublishEvent,
};

pub struct MarketDataStream<C: Clock> {
    utc_clock: C,
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    subscription_tracker: SubscriptionTracker,
    /// Tracks if snapshot has been received for a symbol from level2 channel.
    level2_symbol_status: HashMap<String, bool>,
}

type Result<T> = std::result::Result<T, MarketStreamError>;

impl<C: Clock> MarketDataStream<C> {
    // Coinbase sends heartbeats per second, errors out when no heartbeat is received for 2 sec.
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        clock: C,
    ) -> Self {
        Self {
            utc_clock: clock,
            ev_tx,
            symbol_rx,
            subscription_tracker: SubscriptionTracker::new(),
            level2_symbol_status: Default::default(),
        }
    }

    pub async fn connect(&mut self, jwt_signer: utils::JwtSigner, url: &str) -> Result<()> {
        let request = url.into_client_request().map_err(Box::new)?;
        let (ws_stream, _) = connect_async(request).await.map_err(Box::new)?;
        let (mut write, mut read) = ws_stream.split();

        // TODO: handle "authentication failure" caused by jwt expiration
        let jwt = jwt_signer.sign();
        // Subscribe to heartbeat.
        utils::subscribe_ws(&mut write, "heartbeats", None, &jwt)
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
                            return Err(MarketStreamError::WebSocketConnectionError(
                                "Missing heartbeats".to_string()
                            ));
                    }
                },
                symbol_msg = self.symbol_rx.recv() => {
                    match symbol_msg {
                        Ok(symbol) => {
                            utils::subscribe_ws(&mut write, "level2", Some(&symbol), &jwt)
                            .await
                            .map_err(Box::new)?;
                            utils::subscribe_ws(&mut write, "market_trades", Some(&symbol), &jwt)
                            .await
                            .map_err(Box::new)?;
                        },
                        Err(RecvError::Closed) => {
                            return Ok(());
                        },
                        Err(RecvError::Lagged(num)) => {
                            return Err(MarketStreamError::SubscriptionRequestMissed(
                                format!("{num} subscription requests were missed.")
                            ));
                        },
                    }
                },
                ws_msg = read.next() => {
                    match ws_msg {
                        Some(Ok(Message::Text(text))) => {
                            self.process_ws_stream(text.to_string(), &mut last_heartbeat)?;
                        },
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await.map_err(Box::new)?;
                        },
                        Some(Ok(Message::Close(close_frame))) => {
                            return Err(MarketStreamError::WebSocketConnectionError(
                                format!("WebSocket closed: {close_frame:?}")
                           ));
                        },
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {},
                        Some(Err(error)) => {
                            return Err(MarketStreamError::from(Box::new(error)));
                        },
                        None => {
                            return Err(MarketStreamError::WebSocketConnectionError(
                                "No WebSocket message".to_string()
                            ));
                        },
                    }
                },
            }
        }
    }

    fn process_ws_stream(
        &mut self,
        steam_msg: String,
        last_heartbeat: &mut Option<Instant>,
    ) -> Result<()> {
        match serde_json::from_str::<Stream>(&steam_msg) {
            Ok(Stream::Channel(Channel::Subscriptions { events })) => {
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
            Ok(Stream::Channel(Channel::Heartbeat(heartbeat))) => {
                self.subscription_tracker
                    .track_seq("heartbeat", heartbeat.sequence_num);
                if heartbeat.events.len() != 1 {
                    return Err(MarketStreamError::ProtocolViolation(format!(
                        "Invalid heartbeat {heartbeat:?}"
                    )));
                }
                let event = &heartbeat.events[0];
                self.subscription_tracker
                    .track_heartbeat_counter(event.heartbeat_counter);
                *last_heartbeat = Some(Instant::now());
                // Error out if heartbeat timestamp > 2 seconds behind.
                let now_utc = self.utc_clock.now();
                let age = now_utc.signed_duration_since(event.current_time);
                let delta = chrono::TimeDelta::from_std(Self::HEARTBEAT_INTERVAL).map_err(|e| {
                    MarketStreamError::InternalError(format!("invalid heartbeat interval: {}", e))
                })?;
                if age > delta {
                    return Err(MarketStreamError::ProtocolViolation(format!(
                        "Heartbeat is {} seconds old!",
                        age.num_seconds()
                    )));
                }
                Ok(())
            }
            Ok(Stream::Channel(Channel::Level2(level2))) => self.process_level2_channel(level2),
            Ok(Stream::Channel(Channel::Trade(trade))) => self.process_trade_channel(trade),
            Ok(Stream::Error(Error::ErrorMessage { message })) => {
                Err(MarketStreamError::ServiceError(message))
            }
            Err(err) => Err(MarketStreamError::ServiceError(format!(
                "Couldn't parse message {steam_msg}: {err}"
            ))),
        }
    }

    fn process_level2_channel(&mut self, data: ChannelMessage<Level2>) -> Result<()> {
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
            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL))?;
            for update in event.updates {
                let depth_ev = match update.side {
                    BookSide::Bid => LOCAL_BID_DEPTH_EVENT,
                    BookSide::Ask => LOCAL_ASK_DEPTH_EVENT,
                };

                self.ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: event.product_id.clone(),
                    event: Event {
                        ev: depth_ev,
                        exch_ts: update.event_time.timestamp_nanos_opt().ok_or_else(|| {
                            MarketStreamError::ServiceError(
                                "exchange timestamp out-of-range".into(),
                            )
                        })?,
                        local_ts: self.utc_clock.now().timestamp_nanos_opt().ok_or_else(|| {
                            MarketStreamError::InternalError("local timestamp out-of-range".into())
                        })?,
                        order_id: 0,
                        px: update.price_level,
                        qty: update.new_quantity,
                        ival: 0,
                        fval: 0.0,
                    },
                }))?;
            }
            self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL))?;
        }
        Ok(())
    }

    fn process_trade_channel(&mut self, data: ChannelMessage<Trade>) -> Result<()> {
        self.subscription_tracker
            .track_seq("trade", data.sequence_num);
        for event in data.events {
            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL))?;
            for trade in event.trades {
                let trade_ev = match trade.side {
                    Side::Sell => LOCAL_SELL_TRADE_EVENT,
                    Side::Buy => LOCAL_BUY_TRADE_EVENT,
                };
                self.ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: trade.product_id.clone(),
                    event: Event {
                        ev: trade_ev,
                        exch_ts: trade.time.timestamp_nanos_opt().ok_or_else(|| {
                            MarketStreamError::ServiceError(
                                "exchange timestamp out-of-range".into(),
                            )
                        })?,
                        local_ts: self.utc_clock.now().timestamp_nanos_opt().ok_or_else(|| {
                            MarketStreamError::InternalError("local timestamp out-of-range".into())
                        })?,

                        order_id: 0,
                        px: trade.price,
                        qty: trade.size,
                        ival: 0,
                        fval: 0.0,
                    },
                }))?;
            }
            self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL))?;
        }
        Ok(())
    }
}

/// Tracks missing messages for a WebSocket subscription with sequence numbers and heartbeat
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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use chrono::{DateTime, Duration};
    use hftbacktest::{
        live::ipc::TO_ALL,
        types::{LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_EVENT, LOCAL_BUY_TRADE_EVENT, LiveEvent},
    };
    use serde_json::json;
    use tokio::sync::{broadcast, mpsc};

    use crate::{
        coinbase::{
            MarketStreamError,
            market_data_stream::MarketDataStream,
            utils::{SystemClock, testutils::MockClock},
        },
        connector::PublishEvent,
    };

    #[tokio::test]
    async fn process_level2_messages() {
        let (tx, mut rx) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let mut stream = MarketDataStream::new(tx, sym_rx, SystemClock);
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
            .process_ws_stream(update, &mut last_heartbeat)
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
            .process_ws_stream(snapshot, &mut last_heartbeat)
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
    async fn process_trade_messages() {
        let (tx, mut rx) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let mut stream = MarketDataStream::new(tx, sym_rx, SystemClock);
        let mut last_heartbeat: Option<Instant> = None;

        let test_ts = "2025-06-01T20:32:50Z";
        let test_ts_nano = DateTime::parse_from_rfc3339(test_ts)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();

        let trade = json!({
          "channel": "market_trades",
          "client_id": "",
          "timestamp": "2023-02-09T20:19:35.39625135Z",
          "sequence_num": 2,
          "events": [
            {
              "type": "snapshot",
              "trades": [
                {
                  "trade_id": "000000000",
                  "product_id": "ETH-USD",
                  "price": "1260.01",
                  "size": "0.3",
                  "side": "BUY",
                  "time": test_ts
                }
              ]
            }
          ]
        })
        .to_string();
        stream
            .process_ws_stream(trade, &mut last_heartbeat)
            .expect("expect no error");

        match rx.try_recv().unwrap() {
            PublishEvent::BatchStart(dest) => assert_eq!(dest, TO_ALL),
            other => panic!("expect BatchStart, got {other:?}"),
        }
        match rx.try_recv().unwrap() {
            PublishEvent::LiveEvent(LiveEvent::Feed { event, .. }) => {
                assert_eq!(event.ev, LOCAL_BUY_TRADE_EVENT);
                assert_eq!(event.exch_ts, test_ts_nano);
                assert_eq!(event.px, 1260.01);
                assert_eq!(event.qty, 0.3);
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
        )
    }

    #[tokio::test]
    async fn process_heartbeats() {
        let (tx, _) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let heartbeat_ts = "2025-06-01 20:32:50.121961769 +0000 UTC";
        let heartbeat_dt = DateTime::parse_from_str(heartbeat_ts, "%Y-%m-%d %H:%M:%S%.f %z UTC")
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap();

        let mock_clock = MockClock::new(heartbeat_dt);
        let clock_clone = mock_clock.clone();
        let mut stream = MarketDataStream::new(tx, sym_rx, clock_clone);
        let mut last_heartbeat: Option<Instant> = None;

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

        mock_clock.set_time(heartbeat_dt + Duration::seconds(1));
        stream
            .process_ws_stream(heartbeat, &mut last_heartbeat)
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

        // Heartbeat with old timestamp.
        let heartbeat2 = json!({
          "channel": "heartbeats",
          "client_id": "",
          "timestamp": "2023-06-23T20:31:26.122969572Z",
          "sequence_num": 2,
          "events": [
            {
              "current_time": format!("{heartbeat_ts} m=+91717.525857105"),
              "heartbeat_counter": 3050
            }
          ]
        })
        .to_string();

        mock_clock.set_time(heartbeat_dt + Duration::seconds(3));

        assert!(
            stream
                .process_ws_stream(heartbeat2, &mut last_heartbeat)
                .is_err(),
            "expect stale heartbeat error"
        )
    }

    #[tokio::test]
    async fn process_authentication_failure() {
        let (tx, _) = mpsc::unbounded_channel::<PublishEvent>();
        let (_sym_tx, sym_rx) = broadcast::channel(1);
        let mut stream = MarketDataStream::new(tx, sym_rx, SystemClock);
        let mut last_heartbeat: Option<Instant> = None;
        let error_msg = json!({"type":"error","message":"authentication failure"}).to_string();
        let res = stream.process_ws_stream(error_msg, &mut last_heartbeat);
        assert!(
            matches!(
                res,
                Err(MarketStreamError::ServiceError(ref msg))
                    if msg == "authentication failure",
            ),
            "expect Err(WebSocketStreamError(\"authentication failure\")), got {res:?}"
        );
    }
}
