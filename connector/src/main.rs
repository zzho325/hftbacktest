use std::{
    collections::{HashMap, hash_map::Entry},
    fs::read_to_string,
    panic,
    process::exit,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use clap::Parser;
use hftbacktest::{
    live::ipc::{
        TO_ALL,
        iceoryx::{ChannelError, IceoryxBuilder},
    },
    prelude::*,
};
use iceoryx2::{
    node::NodeBuilder,
    prelude::{SignalHandlingMode, ipc},
};
use tokio::{
    runtime::Builder,
    select,
    signal,
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};
use tracing::error;

use crate::{
    binancefutures::BinanceFutures,
    bybit::Bybit,
    coinbase::Coinbase,
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    fuse::FusedHashMapMarketDepth,
};

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "coinbase")]
pub mod coinbase;

mod connector;
mod fuse;
mod utils;

struct Position {
    qty: f64,
    exch_ts: i64,
}

fn run_receive_task(
    name: &str,
    tx: UnboundedSender<PublishEvent>,
    connector: &mut Box<dyn Connector>,
) -> Result<(), ChannelError> {
    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc::Service>()
        .map_err(|error| ChannelError::BuildError(error.to_string()))?;
    let bot_rx = IceoryxBuilder::new(name).bot(false).receiver()?;
    loop {
        let cycle_time = Duration::from_nanos(1000);
        match node.wait(cycle_time) {
            Ok(()) => {
                while let Some((id, ev)) = bot_rx.receive()? {
                    match ev {
                        LiveRequest::Order {
                            symbol: asset,
                            order,
                        } => match order.req {
                            Status::New => {
                                // Requests to the Connector submit the new order.
                                connector.submit(asset, order, tx.clone());
                            }
                            Status::Canceled => {
                                // Requests to the Connector cancel the order.
                                connector.cancel(asset, order, tx.clone());
                            }
                            status => {
                                error!(?status, "An invalid request was received from the bot.");
                            }
                        },
                        LiveRequest::RegisterInstrument {
                            symbol,
                            tick_size,
                            lot_size: _,
                        } => {
                            // Makes prepare the publisher thread to also add the instrument.
                            tx.send(PublishEvent::RegisterInstrument {
                                id,
                                symbol: symbol.clone(),
                                tick_size,
                            })
                            .unwrap();
                            // Requests to the Connector subscribe to the necessary feeds for the
                            // instrument.
                            connector.register(symbol);
                        }
                    }
                }
            }
            Err(_error) => {
                break;
            }
        }
    }
    Ok(())
}

async fn run_publish_task(
    name: &str,
    order_manager: Arc<Mutex<dyn GetOrders>>,
    mut rx: UnboundedReceiver<PublishEvent>,
    shutdown_signal: Arc<Notify>,
) -> Result<(), ChannelError> {
    let mut depth = HashMap::new();
    let mut position: HashMap<String, Position> = HashMap::new();
    let bot_tx = IceoryxBuilder::new(name).bot(false).sender()?;

    loop {
        select! {
            _ = shutdown_signal.notified() => {
                break;
            }
            Some(msg) = rx.recv() => {
                match msg {
                    PublishEvent::RegisterInstrument {
                        id,
                        symbol,
                        tick_size,
                    } => {
                        // Sends the current state (orders, position, and market depth) to the bot that
                        // requested to add this instrument in batch mode.
                        bot_tx.send(id, &LiveEvent::BatchStart)?;

                        for order in order_manager.lock().unwrap().orders(Some(symbol.clone())) {
                            bot_tx.send(
                                id,
                                &LiveEvent::Order {
                                    symbol: symbol.clone(),
                                    order,
                                },
                            )?;
                        }

                        if let Some(position) = position.get(&symbol) {
                            bot_tx.send(
                                id,
                                &LiveEvent::Position {
                                    symbol: symbol.clone(),
                                    qty: position.qty,
                                    exch_ts: position.exch_ts,
                                },
                            )?;
                        }

                        match depth.entry(symbol) {
                            Entry::Occupied(mut entry) => {
                                let depth_: &mut FusedHashMapMarketDepth = entry.get_mut();
                                let snapshot = depth_.snapshot();
                                for event in snapshot {
                                    bot_tx.send(
                                        id,
                                        &LiveEvent::Feed {
                                            symbol: entry.key().clone(),
                                            event,
                                        },
                                    )?;
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(FusedHashMapMarketDepth::new(tick_size));
                            }
                        }

                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                    PublishEvent::LiveEvent(ev) => {
                        // The live event will only be published if the result is true.
                        if handle_ev(&ev, &mut depth, &mut position) {
                            bot_tx.send(TO_ALL, &ev)?;
                        }
                    }
                    PublishEvent::BatchStart(id) => {
                        bot_tx.send(id, &LiveEvent::BatchStart)?;
                    }
                    PublishEvent::BatchEnd(id) => {
                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Maintains the market depth for all added instruments, allowing another bot to request the same
/// instrument and publishing the market depth snapshot, and fuses the market depth from different
/// streams, such as L1 or L2 with varying depths and update frequencies, to provide the most
/// granular and frequent updates.
///
/// Returns true when the received live event needs to be published; otherwise, it does not.
/// For example, publication is unnecessary if the received market depth data is outdated by more
/// recent data from a different stream due to fusion.
fn handle_ev(
    ev: &LiveEvent,
    depth: &mut HashMap<String, FusedHashMapMarketDepth>,
    position: &mut HashMap<String, Position>,
) -> bool {
    match ev {
        LiveEvent::Feed { symbol, event } => {
            if event.is(BUY_EVENT | DEPTH_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return false,
                    }
                };
                return depth_.update_bid_depth(event.px, event.qty, event.exch_ts);
            } else if event.is(SELL_EVENT | DEPTH_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return false,
                    }
                };
                return depth_.update_ask_depth(event.px, event.qty, event.exch_ts);
            } else if event.is(BUY_EVENT | DEPTH_BBO_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return false,
                    }
                };
                return depth_.update_best_bid(event.px, event.qty, event.exch_ts);
            } else if event.is(SELL_EVENT | DEPTH_BBO_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return false,
                    }
                };
                return depth_.update_best_ask(event.px, event.qty, event.exch_ts);
            } else if event.is(DEPTH_CLEAR_EVENT) {
                let depth_ = {
                    match depth.get_mut(symbol) {
                        Some(d) => d,
                        None => return false,
                    }
                };
                depth_.clear_depth(Side::None, 0.0);
            }
        }
        LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } => {
            if position.contains_key(symbol) {
                let position = position.get_mut(symbol).unwrap();
                return if *exch_ts >= position.exch_ts {
                    position.qty = *qty;
                    true
                } else {
                    false
                };
            } else {
                position.insert(
                    symbol.clone(),
                    Position {
                        qty: *qty,
                        exch_ts: *exch_ts,
                    },
                );
                return true;
            }
        }
        _ => {}
    }
    true
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the connector, used when connecting the bot to the connector.
    name: String,

    /// Connector
    /// * binancefutures: Binance USD-m Futures
    /// * bybit: Bybit Linear Futures
    connector: String,

    /// Connector's configuration file path.
    config: String,
}

#[tokio::main]
async fn main() {
    // Ensures that the main thread will terminate if any of its child threads panics.
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        exit(1);
    }));

    let args = Args::parse();

    tracing_subscriber::fmt::init();

    // Listen for shut down signal and notify publish task.
    let shutdown_signal = Arc::new(Notify::new());
    let shutdown_signal_ = shutdown_signal.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            // Wait for either SIGINT (CTRL+C) or SIGTERM on Unix.
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            select! {
                _ = signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            // Non-Unix platforms only has SIGINT.
            if let Err(error) = signal::ctrl_c().await {
                error!(?error, "Couldn't listen for shutdown signal.");
            }
        }
        shutdown_signal_.notify_waiters();
    });

    let (pub_tx, pub_rx) = unbounded_channel();

    let config = read_to_string(&args.config)
        .map_err(|error| {
            error!(
                ?error,
                config = args.config,
                "An error occurred while reading the configuration file."
            );
        })
        .unwrap();

    let mut connector: Box<dyn Connector> = match args.connector.as_str() {
        "binancefutures" => {
            let mut connector = BinanceFutures::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the BinanceFutures connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "bybit" => {
            let mut connector = Bybit::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Bybit connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "coinbase" => {
            let mut connector = Coinbase::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Coinbase connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        connector => {
            error!(%connector, "This connector doesn't exist.");
            exit(1);
        }
    };

    let name = args.name.clone();
    let order_manager = connector.order_manager();
    let handle = thread::spawn(move || {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async move {
            run_publish_task(&name, order_manager, pub_rx, shutdown_signal)
                .await
                .map_err(|error: ChannelError| {
                    error!(
                        ?error,
                        "An error occurred while sending a live event to the bots."
                    );
                })
                .unwrap();
        });
    });

    let name = args.name;
    run_receive_task(&name, pub_tx, &mut connector)
        .map_err(|error| {
            error!(
                ?error,
                "An error occurred while receiving a request from the bots."
            );
        })
        .unwrap();
    let _ = handle.join();
}
