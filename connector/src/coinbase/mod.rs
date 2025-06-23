mod market_data_stream;
mod msg;
mod ordermanager;
mod rest;
#[cfg(test)]
mod rest_test;
mod utils;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use hftbacktest::{
    prelude::get_precision,
    types::{ErrorKind, LiveError, LiveEvent, OrdType, Order, Status, TimeInForce, Value},
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, Sender},
    mpsc::UnboundedSender,
};
use tokio_tungstenite::tungstenite;
use tracing::{error, warn};

use crate::{
    coinbase::{
        msg::rest::{OrderConfiguration, OrderRequest},
        ordermanager::{OrderManager, SharedOrderManager},
        rest::{CoinbaseClient, CoinbaseClientImpl},
        utils::SystemClock,
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

/// Top-level error aggregating all sub-systems
#[derive(Error, Debug)]
pub enum CoinbaseError {
    #[error("Invalid config: {0}")]
    ConfigError(#[from] toml::de::Error),

    #[error("REST client: {0}")]
    Rest(#[from] RestClientError),

    #[error("Market data stream: {0}")]
    MarketStream(#[from] MarketStreamError),

    #[error("Request {order:?} failed: {source}")]
    Order {
        order: Order,
        #[source]
        source: OrderError,
    },
}

#[derive(Error, Debug)]
pub enum RestClientError {
    /// Network-level failures (HTTP send, connection issues)
    #[error("NetworkError: {0}")]
    NetworkError(#[from] reqwest::Error),

    /// Http non-200 reponse or failure parseing response
    #[error("ServiceError: status = {status}, body = {body}")]
    ServiceError {
        status: reqwest::StatusCode,
        body: String,
    },

    /// Internal errors (url parsing, header construction)
    #[error("InternalError: {0}")]
    InternalError(String),
}

#[derive(Error, Debug)]
pub enum MarketStreamError {
    #[error("SubscriptionRequestMissed: {0}")]
    SubscriptionRequestMissed(String),

    /// Stream violated the expected streaming protocol
    #[error("ProtocolViolation: {0}")]
    ProtocolViolation(String),

    /// Server-sent error message or failure parsing message
    #[error("ServiceError: {0}")]
    ServiceError(String),

    /// Connection-level failures such as transport errors or interruptions
    #[error("WebSocketConnectionError: {0}")]
    WebSocketConnectionError(String),

    /// Internal errors
    #[error("InternalError: {0}")]
    InternalError(String),
}

#[derive(Error, Debug)]
pub enum OrderError {
    /// Invalid requests such as same order exists
    #[error("InvalidRequest: {0}")]
    InvalidRequest(String),

    #[error("InternalError: {0}")]
    InternalError(String),
}

impl From<Box<tungstenite::Error>> for MarketStreamError {
    fn from(err: Box<tungstenite::Error>) -> Self {
        MarketStreamError::WebSocketConnectionError(format!("WebSocket transport error: {err}"))
    }
}

impl From<tokio::sync::mpsc::error::SendError<PublishEvent>> for MarketStreamError {
    fn from(err: tokio::sync::mpsc::error::SendError<PublishEvent>) -> Self {
        MarketStreamError::InternalError(format!("Tokio send error: {err}"))
    }
}

impl From<CoinbaseError> for Value {
    fn from(value: CoinbaseError) -> Value {
        Value::String(value.to_string())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct Config {
    public_ws_url: String,
    private_ws_url: String,
    rest_api_url: String,
    api_key_name: String,
    api_key_secret: String,
    #[serde(default)]
    order_prefix: String,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// A connector for Coinbase Exchange.
pub struct Coinbase<C: CoinbaseClient> {
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>, // Channel to subscribe to symbol.
    order_manager: SharedOrderManager,
    client: C,
}

impl ConnectorBuilder for Coinbase<CoinbaseClientImpl> {
    type Error = CoinbaseError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        let client = CoinbaseClientImpl::new(
            utils::JwtSigner::new(&config.api_key_name, &config.api_key_secret),
            &config.rest_api_url,
        );
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let (symbol_tx, _) = broadcast::channel(500);

        Ok(Coinbase {
            config,
            symbols: Default::default(),
            symbol_tx,
            order_manager,
            client,
        })
    }
}

impl<C: CoinbaseClient> Coinbase<C> {
    pub fn connect_market_data_stream(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let public_ws_url = self.config.public_ws_url.clone();
        let api_key_name = self.config.api_key_name.clone();
        let api_key_secret = self.config.api_key_secret.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|e: CoinbaseError| {
                    // TODO: instead of panic, handle error gracefully
                    if let CoinbaseError::MarketStream(MarketStreamError::InternalError(msg)) = &e {
                        panic!("MarketStream internal error: {msg}");
                    }
                    error!("An error occurred in the market data stream connection: {e}");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            e.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = market_data_stream::MarketDataStream::new(
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                        SystemClock,
                    );
                    stream
                        .connect(
                            utils::JwtSigner::new(&api_key_name, &api_key_secret),
                            &public_ws_url,
                        )
                        .await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl<C: CoinbaseClient> Connector for Coinbase<C> {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_market_data_stream(ev_tx.clone());
    }

    fn submit(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
        let order_manager = self.order_manager.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            // create order in orger manager and keep client side id
            let client_order_id = match order_manager
                .lock()
                .unwrap()
                .create_order(symbol.clone(), order.clone())
            {
                Ok(id) if id.is_empty() => {
                    warn!(
                        ?order,
                        "Coincidentally, creates a duplicated client order id. \
                        This order request will be expired."
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                        .unwrap();
                    return;
                }
                Ok(id) => id,
                Err(e) => {
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        CoinbaseError::Order { source: e, order }.into(),
                    ))))
                    .unwrap();
                    return;
                }
            };

            let send_invalid_event = |reason: String| {
                tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                    ErrorKind::OrderError,
                    CoinbaseError::Order {
                        source: OrderError::InvalidRequest(reason),
                        order: order.clone(),
                    }
                    .into(),
                ))))
                .unwrap()
            };

            let side = match order.side {
                hftbacktest::types::Side::Buy => msg::Side::Buy,
                hftbacktest::types::Side::Sell => msg::Side::Sell,
                _ => {
                    send_invalid_event("invalid side".into());
                    return;
                }
            };
            let quote_size = order.qty.to_string();
            let order_config = match (order.order_type, order.time_in_force) {
                (OrdType::Market, TimeInForce::IOC) => OrderConfiguration::MarketIOC { quote_size },
                (OrdType::Limit, tif) => {
                    let limit_price = format!(
                        "{:.prec$}",
                        order.price_tick as f64 * order.tick_size,
                        prec = get_precision(order.tick_size),
                    );
                    match tif {
                        TimeInForce::IOC => OrderConfiguration::LimitIOC {
                            quote_size,
                            limit_price,
                        },
                        TimeInForce::GTC => OrderConfiguration::LimitGTC {
                            quote_size,
                            limit_price,
                            post_only: false,
                        },
                        TimeInForce::GTX => OrderConfiguration::LimitGTC {
                            quote_size,
                            limit_price,
                            post_only: true,
                        },
                        TimeInForce::FOK => OrderConfiguration::LimitFOK {
                            quote_size,
                            limit_price,
                        },
                        tif => {
                            send_invalid_event(format!(
                                "unsupported order type/TIF: {:?}/{tif:?}",
                                OrdType::Limit,
                            ));
                            return;
                        }
                    }
                }
                (ot, tif) => {
                    send_invalid_event(format!("unsupported order type/TIF: {ot:?}/{tif:?}"));
                    return;
                }
            };

            let _ = client
                .submit_order(OrderRequest {
                    client_order_id,
                    product_id: symbol,
                    side,
                    order_config,
                })
                .await;
            // TODO: update order manager and tx
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {}
}
