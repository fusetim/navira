use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get};

use crate::datastore::DataStore;

pub mod datastore;
mod gateway;
mod index;

async fn index() -> impl IntoResponse {
    (StatusCode::OK, "Welcome to navira-store!").into_response()
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok").into_response()
}

/// Starts the navira-store gateway server
///
/// This function will never return unless the server is stopped or encounters an error.
pub async fn start_server(store: Arc<DataStore>, port: u16) -> Result<(), std::io::Error> {
    let app = gateway::new_ipfs().with_state(store);
    let app = app.route("/", get(index)).route("/health", get(health));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0".to_string(), port)).await?;
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
