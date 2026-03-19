use axum::{
    extract::{State, WebSocketUpgrade,},
    extract::ws::{Message, WebSocket, rejection::WebSocketUpgradeRejection},
    http::{Method, StatusCode, Uri, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

#[derive(Clone)]
struct MockResponse {
    status: u16,
    content_type: String,
    payload: String,
}

struct SharedState {
    // Maps (Method, Path) -> MockResponse
    rest_routes: RwLock<HashMap<(String, String), MockResponse>>,
    // Maps Path -> List of JSON messages to push on connect
    ws_routes: RwLock<HashMap<String, Vec<String>>>,
    // Maps Path -> Number of times the endpoint was hit
    hit_counts: RwLock<HashMap<String, usize>>,
}

#[pyclass]
pub struct MockServer {
    host: String,
    port: u16,
    bound_port: Arc<Mutex<Option<u16>>>,
    state: Arc<SharedState>,
    rt: Runtime,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[pymethods]
impl MockServer {
    #[new]
    #[pyo3(signature = (host = "127.0.0.1", port = 0))]
    fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            bound_port: Arc::new(Mutex::new(None)),
            state: Arc::new(SharedState {
                rest_routes: RwLock::new(HashMap::new()),
                ws_routes: RwLock::new(HashMap::new()),
                hit_counts: RwLock::new(HashMap::new()),
            }),
            // The MockServer gets its own tiny runtime to avoid interfering with OsintEngine
            rt: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("Failed to initialize MockServer runtime"),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    #[pyo3(signature = (method, path, status_code=200, json_payload=None, raw_payload=None))]
    fn add_rest_route(
        &self,
        method: &str,
        path: &str,
        status_code: u16,
        json_payload: Option<&str>,
        raw_payload: Option<&str>,
    ) {
        let (content_type, payload) = if let Some(json) = json_payload {
            ("application/json", json)
        } else if let Some(raw) = raw_payload {
            // Can be text, XML, CSV, etc.
            ("text/plain", raw) 
        } else {
            ("text/plain", "")
        };

        self.state.rest_routes.write().unwrap().insert(
            (method.to_uppercase(), path.to_string()),
            MockResponse {
                status: status_code,
                content_type: content_type.to_string(),
                payload: payload.to_string(),
            },
        );
        tracing::debug!("Mock route added: {} {}", method.to_uppercase(), path);
    }

    fn add_ws_route(&self, path: &str, messages: Vec<String>) {
        self.state.ws_routes.write().unwrap().insert(path.to_string(), messages);
        tracing::debug!("Mock WS route added: {}", path);
    }

    fn get_request_count(&self, path: &str) -> usize {
        *self.state.hit_counts.read().unwrap().get(path).unwrap_or(&0)
    }

    fn http_url(&self) -> String {
        let port = self.bound_port.lock().unwrap().expect("Server not started");
        format!("http://{}:{}", self.host, port)
    }

    fn ws_url(&self) -> String {
        let port = self.bound_port.lock().unwrap().expect("Server not started");
        format!("ws://{}:{}", self.host, port)
    }

    fn start(&self) {
        let (tx, rx) = oneshot::channel();
        *self.shutdown_tx.lock().unwrap() = Some(tx);

        let state = self.state.clone();
        let host = self.host.clone();
        let port = self.port;
        let bound_port_clone = self.bound_port.clone();

        // 1. Create a synchronous channel to block the Python thread
        let (bind_tx, bind_rx) = std::sync::mpsc::channel();

        self.rt.spawn(async move {
            let app = Router::new()
                .fallback(any(handle_request))
                .with_state(state);

            let bind_addr = format!("{}:{}", host, port);
            let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
            
            let actual_port = listener.local_addr().unwrap().port();
            *bound_port_clone.lock().unwrap() = Some(actual_port);
            
            // 2. Signal to the main thread that the port is locked in
            let _ = bind_tx.send(());

            tracing::info!("MockServer live at http://{}:{}", host, actual_port);

            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    rx.await.ok();
                })
                .await
                .unwrap();
                
            tracing::info!("MockServer gracefully shut down.");
        });

        // 3. Block Python from returning from `start()` until the signal is received
        bind_rx.recv().expect("MockServer background thread panicked during startup");
    }

    fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

/// The universal handler that intercepts all requests, increments the counter,
/// and returns the configured payload.
async fn handle_request(
    State(state): State<Arc<SharedState>>,
    // Option<WebSocketUpgrade> swallows the rejection if headers are missing, 
    // cleanly returning None for standard REST requests.
    method: Method,
    uri: Uri,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let path = uri.path().to_string();
    
    // 1. Increment hit count
    *state.hit_counts.write().unwrap().entry(path.clone()).or_insert(0) += 1;

    // 2. Handle WebSocket Upgrades
    if let Ok(ws_upgrade) = ws {
        let is_ws_route = state.ws_routes.read().unwrap().contains_key(&path);
        if is_ws_route {
            let messages = state.ws_routes.read().unwrap().get(&path).unwrap().clone();
            tracing::debug!("Upgrading to WebSocket for {}", path);
            
            return ws_upgrade.on_upgrade(move |mut socket: WebSocket| async move {
                for msg in messages {
                    // We map the String to the appropriate format. 
                    // `.into()` handles compatibility for both Axum 0.7 (String) and 0.8 (Utf8Bytes).
                    if socket.send(Message::from(msg)).await.is_err() {
                        tracing::warn!("Client disconnected before receiving all WS messages");
                        break;
                    }
                }
            });
        }
    }

    // 3. Handle REST Requests
    let route_key = (method.as_str().to_string(), path.clone());
    let mock_res = state.rest_routes.read().unwrap().get(&route_key).cloned();

    if let Some(res) = mock_res {
        let mut response = res.payload.into_response();
        *response.status_mut() = StatusCode::from_u16(res.status).unwrap_or(StatusCode::OK);
        response.headers_mut().insert(
            CONTENT_TYPE,
            axum::http::HeaderValue::from_str(&res.content_type).unwrap(),
        );
        response
    } else {
        tracing::warn!("MockServer: No route configured for {} {}", method, path);
        StatusCode::NOT_FOUND.into_response()
    }
}