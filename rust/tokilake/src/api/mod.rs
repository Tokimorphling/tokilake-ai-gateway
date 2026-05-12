use crate::gateway::{ChannelEntry, ChannelIndex};
use axum::{
    Json, Router,
    extract::{Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokilake_core::{
    error::{ErrorMessage, TunnelError},
    protocol::*,
    session::{ChannelBindParams, GatewaySession, InFlightRequest, SessionManager},
};
use tokilake_smux::Session as SmuxSession;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub start_time:      std::time::Instant,
    pub gateway_config:  crate::gateway::GatewayConfig,
    pub session_manager: Arc<SessionManager<SmuxSession>>,
    pub index:           Arc<ChannelIndex>,
    pub token:           String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    let shared = Arc::new(state);

    // ComfyUI routes — protected by Bearer token auth
    let comfyui_routes = Router::new()
        .route("/workflows", get(comfyui_workflows_list))
        .route("/workflows/:id", get(comfyui_workflow_get))
        .route("/workflows/:id/run", post(comfyui_workflow_run))
        .route("/tasks/:id", get(comfyui_task_get))
        .route("/prompt", post(comfyui_prompt))
        .route("/view", get(comfyui_view))
        .route("/queue", get(comfyui_queue_get))
        .route("/interrupt", post(comfyui_interrupt))
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            bearer_auth_middleware,
        ))
        .with_state(shared.clone());

    Router::new()
        .route("/health", get(health))
        .route("/connect", get(ws_handler))
        .route("/api/tokilake/connect", get(ws_handler))
        .route("/v1/chat/completions", post(chat_completions))
        .nest("/v1/comfyui", comfyui_routes)
        .with_state(shared)
}

/// Middleware that validates Bearer token from Authorization header or query parameter.
async fn bearer_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    let token = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            let s = s.trim();
            if s.to_lowercase().starts_with("bearer ") {
                s[7..].trim()
            } else {
                s
            }
        })
        .unwrap_or("");

    let token = token.strip_prefix("sk-").unwrap_or(token);
    let expected = state.token.strip_prefix("sk-").unwrap_or(&state.token);

    if token.is_empty() || token != expected {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized: invalid or missing token"})),
        )
            .into_response();
    }

    next.run(req).await.into_response()
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "uptime": state.start_time.elapsed().as_secs(),
        "sessions": state.session_manager.session_count(),
    }))
}

// ---------------------------------------------------------------------------
// WebSocket handler
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ConnectQuery {
    token:        Option<String>,
    access_token: Option<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ConnectQuery>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let token_key = match extract_token_from_request(&state.token, &query, &headers) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    ws.protocols(["tokilake.v1"])
        .on_upgrade(move |socket| handle_ws_connection(socket, state, token_key, addr.to_string()))
}

fn extract_token_from_request(
    expected: &str,
    query: &ConnectQuery,
    headers: &axum::http::HeaderMap,
) -> Result<String, TunnelError> {
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            let auth_str = auth_str.trim();
            let token = if auth_str.to_lowercase().starts_with("bearer ") {
                auth_str[7..].trim()
            } else {
                auth_str
            };
            if !token.is_empty() {
                let token = token.strip_prefix("sk-").unwrap_or(token);
                let expected = expected.strip_prefix("sk-").unwrap_or(expected);
                if token == expected {
                    return Ok(token.into());
                }
            }
        }
    }

    let token = query
        .token
        .as_deref()
        .or(query.access_token.as_deref())
        .unwrap_or("");
    let token = token.trim().strip_prefix("sk-").unwrap_or(token.trim());
    let expected = expected.strip_prefix("sk-").unwrap_or(expected);

    if token.is_empty() || token != expected {
        return Err(TunnelError::auth_failed("invalid token"));
    }
    Ok(token.into())
}

// ---------------------------------------------------------------------------
// WebSocket connection lifecycle
// ---------------------------------------------------------------------------

async fn handle_ws_connection(
    socket: axum::extract::ws::WebSocket,
    state: Arc<AppState>,
    token_key: String,
    remote_addr: String,
) {
    let session =
        state
            .session_manager
            .new_session(None, token_key, remote_addr, "websocket".into());

    if let Err(e) = serve_session(&state, &session, socket).await {
        warn!("session error: {e}");
    }

    let guard = session.read().await;
    if let Some(ref info) = guard.worker_info {
        state.index.remove(info.channel_id as i64);
    }
    state.session_manager.release(&guard).await;
}

struct WebSocketStream {
    rx:     mpsc::Receiver<Vec<u8>>,
    tx:     mpsc::Sender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl WebSocketStream {
    fn new(rx: mpsc::Receiver<Vec<u8>>, tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            rx,
            tx,
            buffer: Vec::new(),
        }
    }
}

impl tokio::io::AsyncRead for WebSocketStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !self.buffer.is_empty() {
            let n = buf.remaining().min(self.buffer.len());
            buf.put_slice(&self.buffer[..n]);
            self.buffer.drain(..n);
            return std::task::Poll::Ready(Ok(()));
        }
        match self.rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(data)) if data.is_empty() => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Some(data)) => {
                let n = buf.remaining().min(data.len());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.buffer.extend_from_slice(&data[n..]);
                }
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl tokio::io::AsyncWrite for WebSocketStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.tx.try_send(buf.to_vec()) {
            Ok(()) => std::task::Poll::Ready(Ok(buf.len())),
            Err(mpsc::error::TrySendError::Full(_)) => std::task::Poll::Ready(Ok(buf.len())),
            Err(_) => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "channel closed",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// Control message loop
// ---------------------------------------------------------------------------

async fn serve_session(
    state: &AppState,
    session: &Arc<RwLock<GatewaySession<SmuxSession>>>,
    socket: axum::extract::ws::WebSocket,
) -> Result<(), TunnelError> {
    use axum::extract::ws::Message;

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (ws_out_tx, mut ws_out_rx) = mpsc::channel::<Vec<u8>>(32);
    let (ws_in_tx, ws_in_rx) = mpsc::channel::<Vec<u8>>(32);

    tokio::spawn(async move {
        while let Some(data) = ws_out_rx.recv().await {
            if ws_sender.send(Message::Binary(data.into())).await.is_err() {
                break;
            }
        }
    });

    let tx_for_in = ws_in_tx.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if tx_for_in.send(data.into()).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    let smux = SmuxSession::server(
        WebSocketStream::new(ws_in_rx, ws_out_tx.clone()),
        tokilake_smux::Config {
            version: 1,
            keep_alive_disabled: true,
            ..Default::default()
        },
    );
    let smux = Arc::new(tokio::sync::Mutex::new(smux));

    {
        let mut s = session.write().await;
        s.control_tx = Some(ws_out_tx);
        s.tunnel_session = Some(Arc::clone(&smux));
        s.authenticated = true;
    }

    let Some(mut control_stream) = smux.lock().await.accept().await else {
        return Err(TunnelError::StreamClosed);
    };

    let mut authenticated = true;
    let mut worker_registered = false;
    let mut worker_id = 0i32;
    let mut buf = Vec::new();

    loop {
        let mut tmp = [0u8; 4096];
        let n = match control_stream.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                warn!("control stream error: {e}");
                break;
            }
        };
        buf.extend_from_slice(&tmp[..n]);

        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<_> = buf.drain(..=pos).collect();
            let text = String::from_utf8_lossy(&line);
            let text = text.trim();
            if text.is_empty() {
                continue;
            }

            let msg = match serde_json::from_str::<ControlMessage>(text) {
                Ok(m) => {
                    info!("control message: type={}", m.msg_type);
                    m
                }
                Err(e) => {
                    warn!("parse error: {e}");
                    continue;
                }
            };

            if let Some(resp) = handle_control_message(
                &state.token,
                &state.index,
                &state.session_manager,
                session,
                &mut authenticated,
                &mut worker_registered,
                &mut worker_id,
                &msg,
            )
            .await
            {
                let mut data = serde_json::to_vec(&resp).unwrap();
                data.push(b'\n');
                if control_stream.write_all(&data).await.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn handle_control_message(
    expected_token: &str,
    index: &ChannelIndex,
    session_manager: &SessionManager<SmuxSession>,
    session: &Arc<RwLock<GatewaySession<SmuxSession>>>,
    authenticated: &mut bool,
    worker_registered: &mut bool,
    worker_id: &mut i32,
    msg: &ControlMessage,
) -> Option<ControlMessage> {
    let rid = msg.request_id.clone().unwrap_or_default();

    match msg.msg_type.as_str() {
        control_type::AUTH => {
            if *authenticated {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("auth_already_completed", "already handled"),
                ));
            }
            let auth = msg.auth.as_ref()?;
            let token = auth
                .token
                .trim()
                .strip_prefix("sk-")
                .unwrap_or(auth.token.trim());
            let expected = expected_token.strip_prefix("sk-").unwrap_or(expected_token);
            if token != expected {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("auth_failed", "invalid token"),
                ));
            }
            *authenticated = true;
            session.write().await.authenticated = true;
            Some(ControlMessage::ack(rid, AckMessage {
                message:    "auth_ok".into(),
                namespace:  String::new(),
                worker_id:  0,
                channel_id: 0,
            }))
        }

        control_type::REGISTER => {
            if !*authenticated {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_authenticated", "auth required"),
                ));
            }
            if *worker_registered {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("register_already_completed", "already handled"),
                ));
            }
            let reg = msg.register.as_ref()?;
            if reg.namespace.trim().is_empty() {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("namespace_required", "namespace is required"),
                ));
            }

            let ch_id = index.channel_count() as i32 + 1;
            let group = if reg.group.trim().is_empty() {
                "default"
            } else {
                &reg.group
            };

            index.upsert(ChannelEntry {
                id:            ch_id as i64,
                name:          reg.namespace.clone(),
                provider:      reg.backend_type.clone(),
                models:        reg.models.join(","),
                base_url:      None,
                api_key:       None,
                status:        1,
                weight:        1,
                group:         group.into(),
                priority:      0,
                model_mapping: None,
            });

            *worker_registered = true;
            *worker_id = ch_id;

            session_manager
                .bind_channel(session, ChannelBindParams {
                    worker_id:    ch_id,
                    channel_id:   ch_id,
                    group:        group.into(),
                    models:       reg.models.clone(),
                    backend_type: reg.backend_type.clone(),
                    status:       1,
                    namespace:    reg.namespace.clone(),
                })
                .await;
            let _ = session_manager
                .claim_namespace(session, &reg.namespace)
                .await;
            info!("worker registered: id={ch_id} namespace={}", reg.namespace);

            Some(ControlMessage::ack(rid, AckMessage {
                message:    "register_ok".into(),
                namespace:  reg.namespace.clone(),
                worker_id:  ch_id,
                channel_id: ch_id,
            }))
        }

        control_type::HEARTBEAT => {
            if !*authenticated {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_authenticated", "auth required"),
                ));
            }
            if !*worker_registered {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_registered", "register required"),
                ));
            }
            let hb = msg.heartbeat.as_ref()?;
            if !hb.current_models.is_empty() {
                if let Some(mut ch) = index.get(*worker_id as i64) {
                    let new_models = hb.current_models.join(",");
                    if ch.models != new_models {
                        ch.models = new_models;
                        index.upsert(ch);
                    }
                }
            }
            let s = session.read().await;
            let info = s.worker_info.as_ref();
            Some(ControlMessage::ack(rid, AckMessage {
                message:    "heartbeat_ok".into(),
                namespace:  info.map_or_else(String::new, |i| i.namespace.clone()),
                worker_id:  info.map_or(0, |i| i.worker_id),
                channel_id: info.map_or(0, |i| i.channel_id),
            }))
        }

        control_type::MODELS_SYNC => {
            if !*authenticated {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_authenticated", "auth required"),
                ));
            }
            if !*worker_registered {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_registered", "register required"),
                ));
            }
            let s = session.read().await;
            let info = s.worker_info.as_ref();
            Some(ControlMessage::ack(rid, AckMessage {
                message:    "models_sync_ok".into(),
                namespace:  info.map_or_else(String::new, |i| i.namespace.clone()),
                worker_id:  info.map_or(0, |i| i.worker_id),
                channel_id: info.map_or(0, |i| i.channel_id),
            }))
        }

        control_type::ACK => None,

        control_type::ERROR => {
            if let Some(err) = &msg.error {
                warn!("tokiame error: code={} message={}", err.code, err.message);
            }
            None
        }

        _ => {
            if !*authenticated {
                return Some(ControlMessage::error_msg(
                    rid,
                    ErrorMessage::new("not_authenticated", "auth required"),
                ));
            }
            Some(ControlMessage::error_msg(
                rid,
                ErrorMessage::new(
                    "unsupported_message_type",
                    format!("unsupported message type: {}", msg.msg_type),
                ),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Chat completions — tunnel routing
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChatQuery {
    namespace: Option<String>,
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ChatQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let namespace = query.namespace.unwrap_or_else(|| "test-worker".into());

    let session = match state.session_manager.get_by_namespace(&namespace) {
        Some(s) => s,
        None => {
            return gateway_error(
                StatusCode::BAD_GATEWAY,
                &format!("namespace '{namespace}' is offline"),
            );
        }
    };

    let (tunnel, session_id, channel_id) = {
        let g = session.read().await;
        match &g.tunnel_session {
            Some(t) => (
                Arc::clone(t),
                g.id,
                g.worker_info.as_ref().map_or(0, |i| i.channel_id),
            ),
            None => {
                return gateway_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("namespace '{namespace}' has no tunnel"),
                );
            }
        }
    };

    let bytes = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            return gateway_error(
                StatusCode::BAD_REQUEST,
                &format!("failed to read body: {e}"),
            );
        }
    };

    let body_json: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return gateway_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let model = body_json
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let is_stream = body_json
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let request_id = format!("{namespace}:relay:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id: request_id.clone(),
        route_kind: route_kind::CHAT_COMPLETIONS.into(),
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        model,
        headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
        is_stream,
        body: bytes.to_vec(),
    };

    state.session_manager.track_request(InFlightRequest {
        request_id: request_id.as_str().into(),
        session_id,
        namespace: namespace.as_str().into(),
        channel_id,
        created_at: std::time::Instant::now(),
    });

    let mut req_data = serde_json::to_vec(&tunnel_req).unwrap();
    req_data.push(b'\n');

    let mut stream = match tunnel.lock().await.open().await {
        Some(s) => s,
        None => {
            state.session_manager.remove_request(&request_id);
            return gateway_error(StatusCode::BAD_GATEWAY, "failed to open data stream");
        }
    };

    if let Err(e) = stream.write_all(&req_data).await {
        state.session_manager.remove_request(&request_id);
        return gateway_error(
            StatusCode::BAD_GATEWAY,
            &format!("failed to send request: {e}"),
        );
    }

    relay_response(stream, &request_id, &state.session_manager).await
}

fn gateway_error(status: StatusCode, msg: &str) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}

async fn relay_response(
    reader: impl tokio::io::AsyncRead + Unpin,
    request_id: &str,
    sm: &SessionManager<SmuxSession>,
) -> axum::response::Response {
    use tokilake_core::codec::TunnelCodec;

    let mut codec = TunnelCodec::new(reader, tokio::io::sink());

    let first_frame =
        match tokio::time::timeout(Duration::from_secs(30), codec.read_response()).await {
            Ok(Ok(Some(resp))) => resp,
            Ok(Ok(None)) => {
                sm.remove_request(request_id);
                return gateway_error(StatusCode::BAD_GATEWAY, "stream closed before response");
            }
            Ok(Err(e)) => {
                sm.remove_request(request_id);
                return gateway_error(StatusCode::BAD_GATEWAY, &format!("read response: {e}"));
            }
            Err(_) => {
                sm.remove_request(request_id);
                return gateway_error(StatusCode::BAD_GATEWAY, "request timeout");
            }
        };

    if let Some(err) = &first_frame.error {
        sm.remove_request(request_id);
        return gateway_error(StatusCode::BAD_GATEWAY, &err.message);
    }

    let status_code = StatusCode::from_u16(first_frame.status_code).unwrap_or(StatusCode::OK);

    let mut body = first_frame.body_chunk.0;
    if !first_frame.eof {
        loop {
            match tokio::time::timeout(Duration::from_secs(30), codec.read_response()).await {
                Ok(Ok(Some(frame))) => {
                    if let Some(err) = &frame.error {
                        sm.remove_request(request_id);
                        return gateway_error(StatusCode::BAD_GATEWAY, &err.message);
                    }
                    body.extend_from_slice(&frame.body_chunk.0);
                    if frame.eof {
                        break;
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    sm.remove_request(request_id);
                    return gateway_error(StatusCode::BAD_GATEWAY, &format!("read response: {e}"));
                }
                Err(_) => {
                    sm.remove_request(request_id);
                    return gateway_error(StatusCode::BAD_GATEWAY, "request timeout");
                }
            }
        }
    }

    sm.remove_request(request_id);

    let mut headers = axum::http::HeaderMap::new();
    for (k, v) in &first_frame.headers {
        if let (Ok(name), Ok(val)) = (
            axum::http::HeaderName::from_bytes(k.as_bytes()),
            axum::http::HeaderValue::from_str(v),
        ) {
            headers.insert(name, val);
        }
    }

    let mut response = (headers, body).into_response();
    *response.status_mut() = status_code;
    response
}

// ---------------------------------------------------------------------------
// ComfyUI endpoints — tunnel routing
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ComfyUIQuery {
    model:     Option<String>,
    namespace: Option<String>,
}

/// Resolve namespace and model from query params + optional JSON body.
fn resolve_comfyui_model_namespace(
    query: &ComfyUIQuery,
    body: Option<&serde_json::Value>,
) -> (String, String) {
    let model = query
        .model
        .clone()
        .or_else(|| body.and_then(|b| b.get("model").and_then(|v| v.as_str()).map(String::from)))
        .unwrap_or_else(|| "default".into());
    let namespace = query
        .namespace
        .clone()
        .unwrap_or_else(|| "test-worker".into());
    (model, namespace)
}

/// Open a tunnel stream, send a TunnelRequest, and relay the response.
async fn tunnel_forward(
    state: &AppState,
    namespace: &str,
    tunnel_req: TunnelRequest,
) -> axum::response::Response {
    let session = match state.session_manager.get_by_namespace(namespace) {
        Some(s) => s,
        None => {
            return gateway_error(
                StatusCode::BAD_GATEWAY,
                &format!("namespace '{namespace}' is offline"),
            );
        }
    };

    let (tunnel, session_id, channel_id) = {
        let g = session.read().await;
        match &g.tunnel_session {
            Some(t) => (
                Arc::clone(t),
                g.id,
                g.worker_info.as_ref().map_or(0, |i| i.channel_id),
            ),
            None => {
                return gateway_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("namespace '{namespace}' has no tunnel"),
                );
            }
        }
    };

    let request_id = tunnel_req.request_id.clone();

    state.session_manager.track_request(InFlightRequest {
        request_id: request_id.as_str().into(),
        session_id,
        namespace: namespace.into(),
        channel_id,
        created_at: std::time::Instant::now(),
    });

    let mut req_data = serde_json::to_vec(&tunnel_req).unwrap();
    req_data.push(b'\n');

    let mut stream = match tunnel.lock().await.open().await {
        Some(s) => s,
        None => {
            state.session_manager.remove_request(&request_id);
            return gateway_error(StatusCode::BAD_GATEWAY, "failed to open data stream");
        }
    };

    if let Err(e) = stream.write_all(&req_data).await {
        state.session_manager.remove_request(&request_id);
        return gateway_error(
            StatusCode::BAD_GATEWAY,
            &format!("failed to send request: {e}"),
        );
    }

    relay_response(stream, &request_id, &state.session_manager).await
}

async fn comfyui_prompt(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let bytes = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            return gateway_error(
                StatusCode::BAD_REQUEST,
                &format!("failed to read body: {e}"),
            );
        }
    };

    let body_json: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return gateway_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let (model, namespace) = resolve_comfyui_model_namespace(&query, Some(&body_json));
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_PROMPT.into(),
        method: "POST".into(),
        path: "/prompt".into(),
        model,
        headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
        is_stream: false,
        body: bytes.to_vec(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_workflows_list(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
) -> impl IntoResponse {
    let (model, namespace) = resolve_comfyui_model_namespace(&query, None);
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_WORKFLOWS_LIST.into(),
        method: "GET".into(),
        path: "/comfyui/workflows".into(),
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_workflow_get(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let (model, namespace) = resolve_comfyui_model_namespace(&query, None);
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_WORKFLOW_GET.into(),
        method: "GET".into(),
        path: format!("/comfyui/workflows/{workflow_id}"),
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_workflow_run(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
    axum::extract::Path(workflow_id): axum::extract::Path<String>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let bytes = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            return gateway_error(
                StatusCode::BAD_REQUEST,
                &format!("failed to read body: {e}"),
            );
        }
    };

    let body_json: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return gateway_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let (model, namespace) = resolve_comfyui_model_namespace(&query, Some(&body_json));
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_WORKFLOW_RUN.into(),
        method: "POST".into(),
        path: format!("/comfyui/workflows/{workflow_id}/run"),
        model,
        headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
        is_stream: false,
        body: bytes.to_vec(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_task_get(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let (model, namespace) = resolve_comfyui_model_namespace(&query, None);
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_TASK_GET.into(),
        method: "GET".into(),
        path: format!("/comfyui/tasks/{task_id}"),
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

#[derive(Deserialize)]
struct ComfyUIViewQuery {
    model:     Option<String>,
    namespace: Option<String>,
    filename:  Option<String>,
    subfolder: Option<String>,
    #[serde(rename = "type")]
    file_type: Option<String>,
}

async fn comfyui_view(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIViewQuery>,
) -> impl IntoResponse {
    let filename = match &query.filename {
        Some(f) if !f.is_empty() => f.clone(),
        _ => return gateway_error(StatusCode::BAD_REQUEST, "filename is required"),
    };

    let model = query.model.clone().unwrap_or_else(|| "default".into());
    let namespace = query
        .namespace
        .clone()
        .unwrap_or_else(|| "test-worker".into());
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let mut path = format!("/view?filename={filename}");
    if let Some(ref sf) = query.subfolder {
        if !sf.is_empty() {
            path.push_str(&format!("&subfolder={sf}"));
        }
    }
    if let Some(ref ft) = query.file_type {
        if !ft.is_empty() {
            path.push_str(&format!("&type={ft}"));
        }
    }

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_VIEW.into(),
        method: "GET".into(),
        path,
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_queue_get(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
) -> impl IntoResponse {
    let (model, namespace) = resolve_comfyui_model_namespace(&query, None);
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_QUEUE_GET.into(),
        method: "GET".into(),
        path: "/queue".into(),
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}

async fn comfyui_interrupt(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ComfyUIQuery>,
) -> impl IntoResponse {
    let (model, namespace) = resolve_comfyui_model_namespace(&query, None);
    let request_id = format!("{namespace}:comfyui:{}", uuid::Uuid::new_v4());

    let tunnel_req = TunnelRequest {
        request_id,
        route_kind: route_kind::COMFYUI_INTERRUPT.into(),
        method: "POST".into(),
        path: "/interrupt".into(),
        model,
        headers: HashMap::new(),
        is_stream: false,
        body: Vec::new(),
    };

    tunnel_forward(&state, &namespace, tunnel_req).await
}
