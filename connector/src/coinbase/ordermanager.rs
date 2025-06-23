use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{Order, Status};
use tracing::warn;

use crate::{
    coinbase::OrderError,
    connector::GetOrders,
    utils::{SymbolOrderId, generate_rand_string},
};

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;
type Result<T> = std::result::Result<T, OrderError>;
type ClientOrderId = String;

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

#[derive(Default)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}

impl OrderManager {
    /// Create order in order manager and return client side order id
    pub fn create_order(&mut self, symbol: String, order: Order) -> Result<String> {
        // error out if order already exists
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return Err(OrderError::InvalidRequest("order exists".into()));
        }

        // generate client side order id and add to orders
        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            // return empty string if coincidently created a duplicate id
            return Ok("".into());
        }
        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order: order.clone(),
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        return Ok(client_order_id);
    }
    // async fn submit_order(
    //     &self,
    //     client_order_id: &str,
    //     symbol: &str,
    //     side: Side,
    //     order_configuration: serde_json::Value,
    // ) -> Result<OrderResponse>;
}
