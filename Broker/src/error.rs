// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error conditions of the protocol (§4), rendered as RFC 9457 problems.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiError {
    Malformed(&'static str),
    UnsupportedVersion,
    NotFound,
    InvalidPriority,
    PartitionLimit,
    PayloadTooLarge,
    Backpressure(&'static str),
    Overloaded,
    ShuttingDown,
}

impl ApiError {
    fn parts(self) -> (&'static str, StatusCode, &'static str) {
        use ApiError::*;
        match self {
            Malformed(d) => ("malformed", StatusCode::BAD_REQUEST, d),
            UnsupportedVersion => (
                "unsupported-version",
                StatusCode::NOT_FOUND,
                "protocol version not served",
            ),
            NotFound => ("not-found", StatusCode::NOT_FOUND, "unknown route"),
            InvalidPriority => (
                "invalid-priority",
                StatusCode::CONFLICT,
                "priority must be within 1..=16",
            ),
            PartitionLimit => (
                "partition-limit",
                StatusCode::CONFLICT,
                "maximum number of partitions reached",
            ),
            PayloadTooLarge => (
                "payload-too-large",
                StatusCode::PAYLOAD_TOO_LARGE,
                "body exceeds the buffer capacity",
            ),
            Backpressure(d) => ("backpressure", StatusCode::TOO_MANY_REQUESTS, d),
            Overloaded => (
                "overloaded",
                StatusCode::SERVICE_UNAVAILABLE,
                "partition actor queue full",
            ),
            ShuttingDown => (
                "shutting-down",
                StatusCode::SERVICE_UNAVAILABLE,
                "server is shutting down",
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (kind, status, detail) = self.parts();
        let body = serde_json::json!({
            "type": format!("urn:armonik:broker:{kind}"),
            "title": kind,
            "status": status.as_u16(),
            "detail": detail,
        });
        let mut resp = (status, body.to_string()).into_response();
        let h = resp.headers_mut();
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        if matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
        ) {
            h.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        resp
    }
}
