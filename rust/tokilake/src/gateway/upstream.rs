//! Upstream forwarding service — forwards requests to the resolved upstream LLM provider.

use super::GatewayRequest;
use futures_util::StreamExt;
use http::Response;
use service_async::{
    MakeService, Service,
    layer::{FactoryLayer, layer_fn},
};
use std::convert::Infallible;

#[derive(Clone)]
pub struct UpstreamService {
    pub client: reqwest::Client,
}

impl Service<GatewayRequest> for UpstreamService {
    type Response = Response<axum::body::Body>;
    type Error = anyhow::Error;

    async fn call(&self, req: GatewayRequest) -> Result<Self::Response, Self::Error> {
        let base_url = req
            .channel
            .base_url
            .as_deref()
            .unwrap_or("https://api.openai.com");
        let api_key = req.channel.api_key.as_deref().unwrap_or("");

        let path = req
            .inner
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let upstream_uri = format!("{}{}", base_url.trim_end_matches('/'), path);

        let (parts, body) = req.inner.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await?;

        // Replace model field with the mapped name
        let final_body = match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            Ok(mut json) => {
                if let Some(obj) = json.as_object_mut() {
                    obj.insert("model".into(), serde_json::Value::String(req.model));
                    serde_json::to_vec(&json).unwrap_or(body_bytes.to_vec())
                } else {
                    body_bytes.to_vec()
                }
            }
            Err(_) => body_bytes.to_vec(),
        };

        let mut req_builder = self
            .client
            .request(parts.method, &upstream_uri)
            .body(final_body);

        for (k, v) in parts.headers.iter() {
            if k != http::header::HOST && k != http::header::AUTHORIZATION {
                req_builder = req_builder.header(k, v);
            }
        }

        if !api_key.is_empty() {
            req_builder = req_builder.header("Authorization", format!("Bearer {api_key}"));
        }

        let res = req_builder.send().await?;

        let status = res.status();
        let mut resp_builder = Response::builder().status(status);
        for (k, v) in res.headers() {
            resp_builder = resp_builder.header(k, v);
        }

        let stream = res.bytes_stream().map(|res| {
            res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        });

        Ok(resp_builder.body(axum::body::Body::from_stream(stream))?)
    }
}

// -- Factory / Layer ----------------------------------------------------------

pub struct UpstreamServiceFactory {
    pub client: reqwest::Client,
}

impl MakeService for UpstreamServiceFactory {
    type Service = UpstreamService;
    type Error = Infallible;

    fn make_via_ref(&self, _old: Option<&Self::Service>) -> Result<Self::Service, Self::Error> {
        Ok(UpstreamService {
            client: self.client.clone(),
        })
    }
}

impl UpstreamService {
    pub fn layer()
    -> impl FactoryLayer<crate::gateway::GatewayConfig, (), Factory = UpstreamServiceFactory> {
        layer_fn(
            |c: &crate::gateway::GatewayConfig, _inner: ()| UpstreamServiceFactory {
                client: c.client.clone(),
            },
        )
    }
}
