use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};
use uuid::Uuid;

use super::RequestId;

pub(super) async fn add_request_id(mut request: Request, next: Next) -> Response {
    let request_id = RequestId(new_request_id());
    request.extensions_mut().insert(request_id.clone());

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id.0) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
        .headers_mut()
        .insert("openai-version", HeaderValue::from_static("2020-10-01"));
    response
}

pub(super) fn new_request_id() -> String {
    format!("req_{}", Uuid::new_v4().simple())
}
