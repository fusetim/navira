use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use cid::multibase::Base;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use navira_ipni::serde_json;

use super::IndexProvider;

pub async fn get_head(
    req_headers: HeaderMap,
    State(index_provider): State<Arc<RwLock<IndexProvider>>>,
) -> impl IntoResponse {
    let guard = index_provider.read().await;
    let Some(head) = guard.signed_head.as_ref() else {
        return (StatusCode::NOT_FOUND, "No announcement available!").into_response();
    };
    let head_cid = (*head.head).to_string_of_base(Base::Base32Lower).unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        "application/vnd.ipld.dag-json".parse().unwrap(),
    );
    headers.insert("ETag", head_cid.parse().unwrap());
    // Cache-Control should reflect how often the provider expects to refresh advertisements:
    // For now, we set it to 1 hour, but this should be configurable in the future.
    // Existing head advertisement can be used for as as a day (stale-while-revalidate.
    headers.insert(
        "Cache-Control",
        "public, max-age=3600, stale-while-revalidate=86400, no-transform"
            .parse()
            .unwrap(),
    );

    // Check if "If-None-Match" header is present and matches the current head CID
    if let Some(if_none_match) = req_headers.get("If-None-Match") {
        if if_none_match.to_str().unwrap_or("") == head_cid {
            return (StatusCode::NOT_MODIFIED, headers, "").into_response();
        }
    }

    (
        StatusCode::OK,
        headers,
        serde_json::to_string(head).unwrap_or_else(|_| "{}".to_string()),
    )
        .into_response()
}

pub async fn get_cid(
    req_headers: HeaderMap,
    Path(cid): Path<String>,
    State(index_provider): State<Arc<RwLock<IndexProvider>>>,
) -> impl IntoResponse {
    let cid = match cid.parse::<cid::Cid>() {
        Ok(cid) => cid,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid CID").into_response(),
    };

    debug!("headers: {:?}", &req_headers);

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
            headers.insert(
                "Cache-Control",
                "public, max-age=31536000, immutable, no-transform"
                    .parse()
                    .unwrap(),
            );
            headers.insert(
                "ETag",
                cid.to_string_of_base(Base::Base32Lower)
                    .unwrap()
                    .parse()
                    .unwrap(),
            );

            // Check if "If-None-Match" header is present and matches the current CID
            if let Some(if_none_match) = req_headers.get("If-None-Match") {
                if if_none_match.to_str().unwrap_or("")
                    == cid.to_string_of_base(Base::Base32Lower).unwrap()
                {
                    return (StatusCode::NOT_MODIFIED, headers, "").into_response();
                }
            }

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
