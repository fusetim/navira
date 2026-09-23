use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::datastore::{DataStore, LocationHint};

const PROBE_CID: &'static str = "bafkqaaa";

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ResponseFormat {
    Car,
    Raw,
    Unknown,
}

/// Determines the response format based on the request's query parameters and headers.
///
/// According to the Trustless Gateway Specification, the response format
/// can be determined by either the "format" query parameter or the "Accept" header.
fn get_format(headers: &HeaderMap, params: &HashMap<String, String>) -> ResponseFormat {
    // Check for query parameter "format=car"
    if let Some(format) = params.get("format") {
        match format.as_str() {
            "car" => return ResponseFormat::Car,
            "raw" => return ResponseFormat::Raw,
            _ => return ResponseFormat::Unknown,
        }
    }

    // Check for Accept header
    if let Some(accept_header) = headers.get("Accept") {
        if let Ok(accept_str) = accept_header.to_str() {
            if accept_str.contains("application/vnd.ipld.raw") {
                return ResponseFormat::Raw;
            } else if accept_str.contains("application/vnd.ipld.car") {
                return ResponseFormat::Car;
            }
        }
    }

    ResponseFormat::Unknown
}

async fn prepare_block(
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    State(store): State<Arc<RwLock<DataStore>>>,
    Path(cid): Path<String>,
) -> Result<(LocationHint, HeaderMap), impl IntoResponse> {
    let mut respheaders = HeaderMap::new();
    let format = get_format(&headers, &params);
    // Reject requests with unknown / car formats, as the gateway only supports raw blocks.
    if format != ResponseFormat::Raw {
        return Err((
            StatusCode::BAD_REQUEST,
            respheaders,
            "Present gateway only support raw blocks.".to_string(),
        ));
    }
    let download = params
        .get("download")
        .map(|v| v == "true" || v.is_empty())
        .unwrap_or(false);
    let mut filename = format!("{}.bin", cid);
    if let Some(param) = params.get("filename") {
        if !param.is_empty() {
            filename = param.clone();
        }
    }
    let content_disp = format!(
        "{}; filename=\"{}\"",
        if download { "attachment" } else { "inline" },
        filename
    );
    let etag_value = format!("\"{cid}.raw\"");

    respheaders.insert("Content-Disposition", content_disp.parse().unwrap());
    respheaders.insert("Content-Type", "application/vnd.ipld.raw".parse().unwrap());
    respheaders.insert("Etag", etag_value.parse().unwrap());
    respheaders.insert(
        "Cache-Control",
        "public, max-age=31536000, immutable, no-transform"
            .parse()
            .unwrap(),
    );

    // Check if the header "If-None-Match" is present and matches the current CID
    if let Some(if_none_match) = headers.get("If-None-Match") {
        if if_none_match.to_str().unwrap_or("") == etag_value {
            return Err((StatusCode::NOT_MODIFIED, respheaders, "".to_string()));
        }
    }

    // Special case for the probe CID, which is used to check if the gateway is alive and what features
    // it supports.
    if cid == PROBE_CID {
        // Probe CID should return 200 and an empty body.
        return Err((StatusCode::OK, respheaders, "".to_string()));
    }

    let cid = match cid.parse::<cid::Cid>() {
        Ok(c) => c,
        Err(_) => {
            respheaders.clear();
            return Err((
                StatusCode::BAD_REQUEST,
                respheaders,
                format!("Invalid CID: {cid}"),
            ));
        }
    };

    if let Some(hint) = store.read().await.contains_cid(&cid).await {
        respheaders.insert(
            "Content-Length",
            hint.metadata().block_length().to_string().parse().unwrap(),
        );
        Ok((hint, respheaders))
    } else {
        respheaders.clear();
        Err((
            StatusCode::NOT_FOUND,
            respheaders,
            format!("Present gateway does not know about: {cid}"),
        ))
    }
}

/// GET /ipfs/<cid>
pub async fn get_block(
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    Path(cid): Path<String>,
    State(store): State<Arc<RwLock<DataStore>>>,
) -> impl IntoResponse {
    match prepare_block(headers, Query(params), State(store.clone()), Path(cid)).await {
        Ok((hint, respheaders)) => match store.read().await.get_block_by_hint(&hint).await {
            Some(block) => (StatusCode::OK, respheaders, block.0).into_response(),
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("CID mysteriously disappeared!"),
            )
                .into_response(),
        },
        Err(resp) => resp.into_response(),
    }
}

/// HEAD /ipfs/<cid>
pub async fn head_block(
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    State(store): State<Arc<RwLock<DataStore>>>,
    Path(cid): Path<String>,
) -> impl IntoResponse {
    match prepare_block(headers, Query(params), State(store.clone()), Path(cid)).await {
        Ok((_, respheaders)) => (StatusCode::OK, respheaders).into_response(),
        Err(resp) => resp.into_response(),
    }
}
