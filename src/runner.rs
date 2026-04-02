use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use wreq::ws::message::Message;

use tokio::task::JoinHandle;
use tokio::sync::mpsc; 

use futures_util::StreamExt; 

use grammers_client::{Client as TgClient, client::UpdatesConfiguration, update::Update, media::Media};
use grammers_client::message::Message as TgMessage;

use grammers_session::{storages::SqliteSession, types::PeerId};
use grammers_mtsender::SenderPool;

use crate::client::HttpClient;

use crate::telegram;

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
                            if source_name == "feodo_tracker" || source_name == "ransomware_live" || source_name == "nga_warnings" {
                                println!("🌐 [NETWORK] Success for '{}': {} bytes fetched", source_name, text.len());
                            }
                            let mut guard = buffer.lock().unwrap();
                            let queue = guard.entry(source_name.clone()).or_insert_with(Vec::new);
                            
                            queue.push(text);
                            
                            if queue.len() > max_buffer_size {
                                let excess = queue.len() - max_buffer_size;
                                queue.drain(0..excess);
                                tracing::warn!("Buffer capped for {}. Python `ing is too slow!", source_name);
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
                                // tracing::info!("Connected to WS: {}", source_name);
                                backoff_sec = 1;

                                // Send the subscription payload if it exists
                                if let Some(msg) = &init_message {
                                    if let Err(e) = ws_stream.send(Message::text(msg.clone())).await {
                                        tracing::error!("Failed to send init_message to {}: {}", source_name, e);
                                    } else {
                                        tracing::info!("Sent subscription payload to {}", source_name);
                                    }
                                }

                                while let Some(Ok(msg)) = ws_stream.next().await {
                                    if let Message::Text(text) = msg {
                                        let mut guard = buffer.lock().unwrap();
                                        let queue = guard.entry(source_name.clone()).or_insert_with(Vec::new);
                                        
                                        queue.push(text.to_string());
                                        
                                        if queue.len() > max_buffer_size {
                                            let excess = queue.len() - max_buffer_size;
                                            queue.drain(0..excess);
                                        }
                                    }
                                }
                                // tracing::warn!("WS stream closed for {}. Attempting reconnect...", source_name);
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


#[derive(Debug)]
pub enum TgCommand {
    FetchHistory { target_channel: String, limit: usize },
}

pub struct TelegramMultiplexer {
    pub api_id: i32,
    pub session_path: String,
    pub channels: Vec<(String, String)>, 
}


async fn process_telegram_message(client: &TgClient, msg: &TgMessage, target_channel: &str,) -> String {

    let mut media_path = String::new();
    
    if let Some(Media::Photo(photo)) = msg.media() {
        let _ = tokio::fs::create_dir_all("osint_media").await;
        let path = format!("osint_media/{}_{}.jpg", target_channel, msg.id());
        
        if let Err(e) = client.download_media(&photo, &path).await {
            tracing::error!("Photo download failed: {}", e);
        } else {
            media_path = path;
        }
    }
    
    // Pass the media path to the synchronous text formatter
    telegram::build_message(msg, target_channel, &media_path)
}


impl TelegramMultiplexer {
    pub fn spawn(
        self,
        buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
        max_buffer_size: usize,
        mut cmd_rx: mpsc::Receiver<TgCommand>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            
            let session = match SqliteSession::open(&self.session_path).await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    tracing::error!("Failed to open Telegram session: {}", e);
                    return;
                }
            };

            let SenderPool { runner, updates, handle } = SenderPool::new(Arc::clone(&session), self.api_id);
            let client = TgClient::new(handle);
            let _task = tokio::spawn(runner.run());

            if !client.is_authorized().await.unwrap_or(false) {
                tracing::error!("Telegram session '{}' is not authorized! Please log in via the UI.", self.session_path);
                return;
            }

            let mut chat_map: HashMap<PeerId, (String, String)> = HashMap::new();

            for (name, target) in &self.channels {
                match client.resolve_username(target).await {
                    Ok(Some(chat)) => {
                        chat_map.insert(chat.id(), (name.clone(), target.clone()));
                        tracing::info!("Successfully resolved Telegram channel: @{}", target);
                    }
                    Ok(None) => tracing::warn!("Could not find Telegram channel: @{}", target),
                    Err(e) => tracing::error!("Error resolving Telegram channel @{}: {}", target, e),
                }
            }

            tracing::info!("Telegram multiplexer live. Listening to {} channels...", chat_map.len());

            let mut update_stream = client.stream_updates(updates, UpdatesConfiguration {
                catch_up: true,
                ..Default::default()
            }).await;

            loop {
                tokio::select! {
                    // LIVE MESSAGES 
                    update_res = update_stream.next() => {
                        match update_res {
                            Ok(Update::NewMessage(msg)) if !msg.outgoing() => {
                                if let Some((source_name, target_channel)) = chat_map.get(&msg.peer_id()) {
                                    
                                    let payload = process_telegram_message(&client, &msg, target_channel).await;
                                    
                                    let mut bg = buffer.lock().unwrap();
                                    let q = bg.entry(source_name.clone()).or_default();
                                    if q.len() < max_buffer_size {
                                        q.push(payload);
                                    }
                                }
                            }
                            Ok(_) => {} 
                            Err(e) => {
                                tracing::error!("Telegram update error: {}", e);
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }

                    // HISTORY COMMANDS 
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            TgCommand::FetchHistory { target_channel, limit } => {
                                tracing::info!("Fetching {} historical messages from @{}", limit, target_channel);
                                
                                let client_clone = client.clone();
                                let buffer_clone = buffer.clone();
                                let source_name = format!("tg_{}", target_channel);
                                
                                tokio::spawn(async move {
                                    match client_clone.resolve_username(&target_channel).await {
                                        Ok(Some(chat)) => {
                                            if let Some(peer_ref) = chat.to_ref().await {
                                                let mut iter = client_clone.iter_messages(peer_ref);
                                                let mut count = 0;
                                                
                                                while let Ok(Some(msg)) = iter.next().await {
                                                    if count >= limit { break; }
                                                    
                                                    let payload = process_telegram_message(&client_clone, &msg, &target_channel).await;

                                                    let mut bg = buffer_clone.lock().unwrap();
                                                    let q = bg.entry(source_name.clone()).or_default();
                                                    if q.len() < max_buffer_size {
                                                        q.push(payload);
                                                    }
                                                    count += 1;
                                                }
                                                tracing::info!("History fetch complete for @{} ({} messages)", target_channel, count);
                                            }
                                        }
                                        _ => tracing::error!("Failed to resolve @{} for history fetch", target_channel),
                                    }
                                });
                            }
                        }
                    }
                }
            }


        })
    }
}