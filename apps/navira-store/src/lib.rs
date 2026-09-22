use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get};
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

use crate::{datastore::DataStore, index::IndexProvider};

pub mod datastore;
mod gateway;
pub mod index;

async fn index() -> impl IntoResponse {
    (StatusCode::OK, "Welcome to navira-store!").into_response()
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok").into_response()
}

/// Starts the navira-store gateway server
///
/// This function will never return unless the server is stopped or encounters an error.
pub async fn start_server(
    store: Arc<RwLock<DataStore>>,
    index_provider: Arc<RwLock<IndexProvider>>,
    port: u16,
) -> Result<(), std::io::Error> {
    let gateway = gateway::new_ipfs_service().with_state(store);
    let ipni = index::new_ipni_service().with_state(index_provider);
    let app = gateway
        .route("/", get(index))
        .route("/health", get(health))
        .merge(ipni)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CompressionLayer::new().gzip(true).no_deflate().no_br()),
        );
    let listener = tokio::net::TcpListener::bind(("0.0.0.0".to_string(), port)).await?;
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
