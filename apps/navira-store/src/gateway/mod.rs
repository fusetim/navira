use std::sync::Arc;

use axum::{Router, routing::{get, head}};

use crate::datastore::DataStore;

mod ipfs;

pub fn new_ipfs() -> Router<Arc<DataStore>> {
    Router::new()
        .route("/ipfs/{cid}", get(ipfs::get_block))
        .route("/ipfs/{cid}", head(ipfs::head_block))
}