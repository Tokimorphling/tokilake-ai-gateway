//! Route service — resolves the requested model to a Channel.

use super::{AuthedRequest, ChannelIndex, ChannelInfo, GatewayRequest};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use service_async::{
    MakeService, Service,
    layer::{FactoryLayer, layer_fn},
};
use std::sync::Arc;

pub struct RouteService<T> {
    pub inner: T,
    pub index: Arc<ChannelIndex>,
}

impl<T> Service<AuthedRequest> for RouteService<T>
where
    T: Service<GatewayRequest, Response = Response<axum::body::Body>, Error = anyhow::Error>,
{
    type Response = Response<axum::body::Body>;
    type Error = anyhow::Error;

    async fn call(&self, req: AuthedRequest) -> Result<Self::Response, Self::Error> {
        let (parts, body) = req.inner.into_parts();
        let bytes = body.collect().await?.to_bytes();

        let model = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from))
            .unwrap_or_else(|| "default".into());

        let Some(ch) = self.index.select(&req.token_group, &model) else {
            return Ok(model_not_found(&model));
        };

        let (mapped_model, _) = ch.map_model(&model);

        let gw_req = GatewayRequest {
            inner:   Request::from_parts(parts, axum::body::Body::from(bytes)),
            model:   mapped_model,
            channel: ChannelInfo::from(&ch),
        };

        self.inner.call(gw_req).await
    }
}

fn model_not_found(model: &str) -> Response<axum::body::Body> {
    let body = serde_json::json!({
        "error": {
            "message": format!("No available channel found for model: {model}"),
            "type": "invalid_request_error",
            "code": "model_not_found",
        }
    });
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

// -- Factory / Layer ----------------------------------------------------------

pub struct RouteServiceFactory<T> {
    inner: T,
    index: Arc<ChannelIndex>,
}

impl<T: MakeService> MakeService for RouteServiceFactory<T> {
    type Service = RouteService<T::Service>;
    type Error = T::Error;

    fn make_via_ref(&self, old: Option<&Self::Service>) -> Result<Self::Service, Self::Error> {
        Ok(RouteService {
            inner: self.inner.make_via_ref(old.map(|o| &o.inner))?,
            index: Arc::clone(&self.index),
        })
    }
}

impl<T> RouteService<T> {
    pub fn layer()
    -> impl FactoryLayer<crate::gateway::GatewayConfig, T, Factory = RouteServiceFactory<T>> {
        layer_fn(
            |c: &crate::gateway::GatewayConfig, inner| RouteServiceFactory {
                inner,
                index: Arc::clone(&c.index),
            },
        )
    }
}
