use std::{
    string,
    sync::{Arc, Mutex},
};

use hftbacktest::types::Order;

use crate::connector::GetOrders;

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub struct OrderManager {}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {}
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        Vec::new()
    }
}
