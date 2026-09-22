use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use navira_ipni::{ad::SignedHead, serde_json};

use super::IndexProvider;

pub async fn get_head(
    State(index_provider): State<Arc<RwLock<IndexProvider>>>,
) -> impl IntoResponse {
    match index_provider.read().await.signed_head.as_ref() {
        Some(head) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                "Content-Type",
                "application/vnd.ipld.dag-json".parse().unwrap(),
            );
            (
                StatusCode::OK,
                headers,
                serde_json::to_string(head).unwrap_or_else(|_| "{}".to_string()),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "No announcement available!").into_response(),
    }
}

pub async fn get_cid(
    Path(cid): Path<String>,
    State(index_provider): State<Arc<RwLock<IndexProvider>>>,
) -> impl IntoResponse {
    let cid = match cid.parse::<cid::Cid>() {
        Ok(cid) => cid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid CID").into_response(),
    };

    match index_provider
        .read()
        .await
        .get_block_from_car(&cid.hash())
        .await
    {
        Ok(Some(entry)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                "Content-Type",
                "application/vnd.ipld.dag-json".parse().unwrap(),
            );
            (StatusCode::OK, headers, entry.block.0).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("No entry found for CID: {cid}"),
        )
            .into_response(),
        Err(_) => (StatusCode::FORBIDDEN, "Access denied").into_response(),
    }
}
