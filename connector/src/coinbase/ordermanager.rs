use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use hftbacktest::types::Order;

use crate::{
    coinbase::{OrderError, rest::RestClient},
    connector::GetOrders,
    utils::SymbolOrderId,
};

pub type SharedOrderManager<C> = Arc<Mutex<OrderManager<C>>>;
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
pub struct OrderManager<C: RestClient> {
    client: C,
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl<C: RestClient> OrderManager<C> {
    pub fn new(client: C, prefix: &str) -> Self {
        Self {
            client,
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }
}

impl<C: RestClient> GetOrders for OrderManager<C> {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}

impl<C: RestClient> OrderManager<C> {
    // let result = self.order_manager.create_order(symbol, order);
    // create id,
    // save order
    // submit to REST
    // handle REST response
    // construct order or error for response
    pub fn create_order(&self, symbol: impl Into<String>, mut order: Order) -> Result<Order> {
        Ok(order)
    }
    // async fn submit_order(
    //     &self,
    //     client_order_id: &str,
    //     symbol: &str,
    //     side: Side,
    //     order_configuration: serde_json::Value,
    // ) -> Result<OrderResponse>;
}
