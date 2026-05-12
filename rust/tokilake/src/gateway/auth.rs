//! Auth service — validates the Bearer token.

use super::AuthedRequest;
use http::{Request, Response, StatusCode};
use service_async::{
    MakeService, Service,
    layer::{FactoryLayer, layer_fn},
};

pub struct AuthService<T> {
    pub inner: T,
    pub db:    toasty::Db,
}

impl<T> Service<Request<axum::body::Body>> for AuthService<T>
where
    T: Service<AuthedRequest, Response = Response<axum::body::Body>, Error = anyhow::Error>,
{
    type Response = Response<axum::body::Body>;
    type Error = anyhow::Error;

    async fn call(&self, req: Request<axum::body::Body>) -> Result<Self::Response, Self::Error> {
        let Some(token_key) = extract_token(&req) else {
            return Ok(unauthorized(
                "Missing or invalid Authorization. Expected: Bearer <token> or \
                 ?accessToken=<token>",
                "invalid_api_key",
            ));
        };

        use crate::model::Token;
        let mut db = self.db.clone();
        let token = Token::filter_by_key(&token_key).first().exec(&mut db).await;

        let (token_name, _token_group) = match token {
            Ok(Some(t)) if t.status == 1 => (t.name, "default"),
            _ => {
                return Ok(unauthorized(
                    "Invalid or disabled token.",
                    "invalid_api_key",
                ));
            }
        };

        self.inner
            .call(AuthedRequest {
                inner: req,
                token_name,
                token_group: "default".into(),
            })
            .await
    }
}

fn extract_token(req: &Request<axum::body::Body>) -> Option<String> {
    let from_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            let s = s.trim();
            let token = s.strip_prefix("Bearer ").unwrap_or(s);
            (!token.is_empty()).then(|| token.to_string())
        });

    let token = from_header.or_else(|| {
        req.uri().query().and_then(|q| {
            q.split('&')
                .find(|p| p.starts_with("accessToken="))
                .and_then(|p| p.split('=').nth(1))
                .map(|s| s.to_string())
        })
    })?;

    let stripped = token.strip_prefix("sk-").unwrap_or(&token);
    (!stripped.is_empty()).then(|| stripped.to_string())
}

fn unauthorized(message: &str, code: &str) -> Response<axum::body::Body> {
    let body = serde_json::json!({
        "error": { "message": message, "type": "invalid_request_error", "code": code }
    });
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

// -- Factory / Layer ----------------------------------------------------------

pub struct AuthServiceFactory<T> {
    inner: T,
    db:    toasty::Db,
}

impl<T: MakeService> MakeService for AuthServiceFactory<T> {
    type Service = AuthService<T::Service>;
    type Error = T::Error;

    fn make_via_ref(&self, old: Option<&Self::Service>) -> Result<Self::Service, Self::Error> {
        Ok(AuthService {
            inner: self.inner.make_via_ref(old.map(|o| &o.inner))?,
            db:    self.db.clone(),
        })
    }
}

impl<T> AuthService<T> {
    pub fn layer()
    -> impl FactoryLayer<crate::gateway::GatewayConfig, T, Factory = AuthServiceFactory<T>> {
        layer_fn(
            |c: &crate::gateway::GatewayConfig, inner| AuthServiceFactory {
                inner,
                db: c.db.clone(),
            },
        )
    }
}
