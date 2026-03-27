use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use futures_util::StreamExt; 

use grammers_client::{Client as TgClient, update::Update};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;

use crate::client::HttpClient;


pub trait ConnectionRunner: Send + Sync {
    fn spawn(
        &self,
        source_name: String,
        target_url: String,
        buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
        max_buffer_size: usize,
    ) -> JoinHandle<()>;
}

pub struct RestRunner {
    pub interval_sec: u64,
    pub headers: Option<HashMap<String, String>>,
    pub client: Arc<dyn HttpClient>,
}

impl ConnectionRunner for RestRunner {
    fn spawn(
        &self,
        source_name: String,
        target_url: String,
        buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
        max_buffer_size: usize,
    ) -> JoinHandle<()> {
        let client = self.client.clone();
        let interval_sec = self.interval_sec;
        let headers = self.headers.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_sec));
            let mut consecutive_errors = 0;

            loop {
                interval.tick().await;
                tracing::debug!("Fetching from {}...", source_name);

                match client.get(&target_url, headers.clone()).await {
                    Ok(bytes) => {
                        consecutive_errors = 0;
                        if let Ok(text) = String::from_utf8(bytes) {
                            let mut guard = buffer.lock().unwrap();
                            let queue = guard.entry(source_name.clone()).or_insert_with(Vec::new);
                            
                            queue.push(text);
                            
                            if queue.len() > max_buffer_size {
                                let excess = queue.len() - max_buffer_size;
                                queue.drain(0..excess);
                                tracing::warn!("Buffer capped for {}. Python polling is too slow!", source_name);
                            }
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        tracing::error!("Failed to fetch {} (Strike {}): {}", source_name, consecutive_errors, e);
                        
                        if consecutive_errors >= 5 {
                            tracing::warn!("Circuit breaker tripped for {}. Sleeping for 5 minutes.", source_name);
                            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                            consecutive_errors = 0;
                        }
                    }
                }
            }
        })
    }
}


pub struct WsRunner {
    pub init_message: Option<String>,
}

impl ConnectionRunner for WsRunner {
    fn spawn(
        &self,
        source_name: String,
        target_url: String,
        buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
        max_buffer_size: usize,
    ) -> JoinHandle<()> {
        let init_message = self.init_message.clone();
        let ws_client = wreq::Client::new();

        tokio::spawn(async move {
            let mut backoff_sec = 1;

            loop {
                tracing::debug!("Connecting WS to {}...", target_url);

                match ws_client.websocket(&target_url).send().await {
                    Ok(response) => {
                        match response.into_websocket().await {
                            Ok(mut ws_stream) => {
                                tracing::info!("Connected to WS: {}", source_name);
                                backoff_sec = 1;

                                // Send the subscription payload if it exists
                                if let Some(msg) = &init_message {
                                    if let Err(e) = ws_stream.send(wreq::ws::message::Message::text(msg.clone())).await {
                                        tracing::error!("Failed to send init_message to {}: {}", source_name, e);
                                    } else {
                                        tracing::info!("Sent subscription payload to {}", source_name);
                                    }
                                }

                                while let Some(Ok(msg)) = ws_stream.next().await {
                                    if let wreq::ws::message::Message::Text(text) = msg {
                                        let mut guard = buffer.lock().unwrap();
                                        let queue = guard.entry(source_name.clone()).or_insert_with(Vec::new);
                                        
                                        queue.push(text.to_string());
                                        
                                        if queue.len() > max_buffer_size {
                                            let excess = queue.len() - max_buffer_size;
                                            queue.drain(0..excess);
                                        }
                                    }
                                }
                                tracing::warn!("WS stream closed for {}. Attempting reconnect...", source_name);
                            }
                            Err(e) => {
                                tracing::error!("WS Upgrade failed for {}: {}. Retrying in {}s", source_name, e, backoff_sec);
                                tokio::time::sleep(std::time::Duration::from_secs(backoff_sec)).await;
                                if backoff_sec < 60 { backoff_sec *= 2; }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("WS Connection failed for {}: {}. Retrying in {}s", source_name, e, backoff_sec);
                        tokio::time::sleep(std::time::Duration::from_secs(backoff_sec)).await;
                        if backoff_sec < 60 { backoff_sec *= 2; }
                    }
                }
            }
        })
    }
}


pub struct TelegramRunner {
    pub api_id: i32,
    pub session_path: String,
}

impl ConnectionRunner for TelegramRunner {
    fn spawn(
        &self,
        source_name: String,
        target_url: String, 
        buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
        max_buffer_size: usize,
    ) -> JoinHandle<()> {
        let api_id = self.api_id;
        let session_path = self.session_path.clone();

        tokio::spawn(async move {
            tracing::info!("Connecting to Telegram MTProto for {}...", source_name);

            let session = Arc::new(SqliteSession::open(&session_path).await.unwrap());
            let pool = SenderPool::new(Arc::clone(&session), api_id);
            let tg_client = TgClient::new(pool.handle);
            tokio::spawn(pool.runner.run());

            // Enforce that the session file exists and is authorized
            if !tg_client.is_authorized().await.unwrap_or(false) {
                tracing::error!("Telegram session {} is not authorized! You must run a local login script first to generate the session file.", session_path);
                return;
            }

            let target_peer = match tg_client.resolve_username(&target_url).await {
                Ok(Some(peer)) => peer,
                Ok(None) => {
                    tracing::error!("Telegram channel @{} not found!", target_url);
                    return;
                }
                Err(e) => {
                    tracing::error!("Error resolving @{}: {}", target_url, e);
                    return;
                }
            };
            let target_id = target_peer.id();

            let mut updates = tg_client.stream_updates(pool.updates, Default::default()).await;
            tracing::info!("Listening for live OSINT on Telegram channel: @{}", target_url);

            while let Ok(update) = updates.next().await {
                if let Update::NewMessage(message) = update {
                    if message.peer().map(|p| p.id()) == Some(target_id) {
                        
                        let json_msg = serde_json::json!({
                            "message_id": message.id(),
                            "channel": target_url,
                            "text": message.text(),
                            "date": message.date().timestamp()
                        });

                        let mut guard = buffer.lock().unwrap();
                        let queue = guard.entry(source_name.clone()).or_insert_with(Vec::new);
                        
                        queue.push(json_msg.to_string());

                        if queue.len() > max_buffer_size {
                            let excess = queue.len() - max_buffer_size;
                            queue.drain(0..excess);
                        }
                    }
                }
            }
            tracing::warn!("Telegram updates stream dropped for {}", source_name);
        })
    }
}