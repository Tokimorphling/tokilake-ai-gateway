//! Tokilake Gateway — Service layer definitions.

pub mod auth;
pub mod channel_index;
pub mod hub;
pub mod route;
pub mod upstream;

pub use channel_index::{ChannelEntry, ChannelIndex};
use http::Request;
use std::sync::Arc;

/// Information about a resolved upstream channel.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub id:            i64,
    pub name:          String,
    pub provider:      String,
    pub base_url:      Option<String>,
    pub api_key:       Option<String>,
    pub models:        String,
    pub weight:        u32,
    pub model_mapping: Option<String>,
}

impl From<&ChannelEntry> for ChannelInfo {
    fn from(ch: &ChannelEntry) -> Self {
        Self {
            id:            ch.id,
            name:          ch.name.clone(),
            provider:      ch.provider.clone(),
            base_url:      ch.base_url.clone(),
            api_key:       ch.api_key.clone(),
            models:        ch.models.clone(),
            weight:        ch.weight,
            model_mapping: ch.model_mapping.clone(),
        }
    }
}

/// A request that has passed authentication.
pub struct AuthedRequest {
    pub inner:       Request<axum::body::Body>,
    pub token_name:  String,
    pub token_group: String,
}

/// A request that has been routed to a specific channel.
pub struct GatewayRequest {
    pub inner:   Request<axum::body::Body>,
    pub model:   String,
    pub channel: ChannelInfo,
}

/// Config struct used by the `FactoryStack`.
#[derive(Clone)]
pub struct GatewayConfig {
    pub db:     toasty::Db,
    pub client: reqwest::Client,
    pub index:  Arc<ChannelIndex>,
}

/// Build the complete gateway service stack.
pub fn build_gateway_stack(
    config: GatewayConfig,
) -> impl service_async::MakeService<
    Service = impl service_async::Service<
        Request<axum::body::Body>,
        Response = http::Response<axum::body::Body>,
        Error = anyhow::Error,
    >,
    Error = std::convert::Infallible,
> {
    use service_async::stack::FactoryStack;

    FactoryStack::new(config)
        .push(upstream::UpstreamService::layer())
        .push(route::RouteService::layer())
        .push(auth::AuthService::layer())
        .into_inner()
}
