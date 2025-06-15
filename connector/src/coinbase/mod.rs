mod market_data_stream;
mod msg;
mod ordermanager;
mod utils;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use anyhow::Error;
use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, Sender},
    mpsc::UnboundedSender,
};
use tokio_tungstenite::tungstenite;
use tracing::error;

use crate::{
    coinbase::ordermanager::{OrderManager, SharedOrderManager},
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

#[derive(Error, Debug)]
pub enum CoinbaseError {
    #[error("SubscriptionRequestMissed: {0}")]
    SubscriptionRequestMissed(String),
    #[error("WebSocketStreamError: {0}")]
    WebSocketStreamError(String),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("WebSocketTransportError: {0:?}")]
    WebSocketTransportError(#[from] Box<tungstenite::Error>),
    #[error("ConnectionInterrupted: {0}")]
    ConnectionInterrupted(String),
}

impl From<CoinbaseError> for Value {
    fn from(value: CoinbaseError) -> Value {
        Value::String(value.to_string())
    }
}

#[derive(Deserialize)]
pub struct Config {
    public_url: String,
    private_url: String,
    key_name: String,
    key_secret: String,
    #[serde(default)]
    order_prefix: String,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// A connector for Coinbase Exchange.
pub struct Coinbase {
    config: Config,
    symbols: SharedSymbolSet,
    symbol_tx: Sender<String>, // Channel to subscribe to symbol.
    order_manager: SharedOrderManager,
}

impl ConnectorBuilder for Coinbase {
    type Error = Error;

    fn build_from(config: &str) -> Result<Self, Error> {
        let config: Config = toml::from_str(config)?;
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let (symbol_tx, _) = broadcast::channel(500);

        Ok(Coinbase {
            config,
            symbols: Default::default(),
            symbol_tx,
            order_manager,
        })
    }
}

impl Coinbase {
    pub fn connect_market_data_stream(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let public_url = self.config.public_url.clone();
        let key_name = self.config.key_name.clone();
        let key_secret = self.config.key_secret.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: CoinbaseError| {
                    error!(
                        ?error,
                        "An error occurred in the market data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = market_data_stream::MarketDataStream::new(
                        // client.clone(),
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                    );
                    stream.connect(&key_name, &key_secret, &public_url).await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl Connector for Coinbase {
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

    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {}

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {}
}
