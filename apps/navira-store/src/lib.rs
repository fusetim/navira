use std::{sync::Arc, time::Duration};

use axum::{http::StatusCode, response::IntoResponse, routing::get};
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::Span;

use crate::{datastore::DataStore, index::IndexProvider};

mod config;
pub mod datastore;
mod gateway;
pub mod index;
pub use config::Config;

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

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &axum::http::Request<_>| {
            tracing::info_span!(
                "request",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version(),
                status = tracing::field::Empty,
                ms = tracing::field::Empty,
            )
        })
        .on_request(|_request: &axum::http::Request<_>, _span: &Span| {
            tracing::info!("request received");
        })
        .on_response(
            |response: &axum::http::Response<_>, latency: Duration, span: &Span| {
                span.record("status", response.status().as_u16());
                span.record("ms", latency.as_millis() as u64);
                tracing::info!("response sent");
            },
        );

    let ipni = index::new_ipni_service().with_state(index_provider);
    let app = gateway
        .route("/", get(index))
        .route("/health", get(health))
        .merge(ipni)
        .layer(
            ServiceBuilder::new()
                .layer(trace_layer)
                .layer(CompressionLayer::new().gzip(true).no_deflate().no_br())
                .layer(CorsLayer::permissive()),
        );
    let listener = tokio::net::TcpListener::bind(("0.0.0.0".to_string(), port)).await?;
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
