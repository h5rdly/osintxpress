use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use futures_util::StreamExt; 

use arrow::record_batch::RecordBatch;

use crate::client::HttpClient; 
use crate::parser::SourceParser;


pub enum ConnectionType {
    Rest { interval_sec: u64 },
    WebSocket,
}

pub struct Source {
    pub name: String,
    pub url: String,
    pub conn_type: ConnectionType,
    pub parser: Box<dyn SourceParser>, 
}

pub struct Engine {
    rt: Runtime,
    active_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    sources: Arc<Mutex<HashMap<String, Source>>>,
    buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
    http_client: Arc<dyn HttpClient>,
}


impl Engine {

    pub fn new(worker_threads: usize, http_client: Arc<dyn HttpClient>) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        Self {
            rt,
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            sources: Arc::new(Mutex::new(HashMap::new())),
            buffer: Arc::new(Mutex::new(HashMap::new())),
            http_client, 
        }
    }


    pub fn add_source(&self, name: String, url: String, conn_type: ConnectionType, parser: Box<dyn SourceParser>) {
        self.sources.lock().unwrap().insert(
            name.clone(), 
            Source { name, url, conn_type, parser }
        );
    }


    pub fn start_source(&self, name: &str) -> Result<(), String> {
        let mut tasks = self.active_tasks.lock().unwrap();
        
        if tasks.contains_key(name) {
            tracing::warn!("Source '{}' is already running.", name);
            return Ok(());
        }

        let sources_guard = self.sources.lock().unwrap();
        let source = sources_guard.get(name).ok_or_else(|| format!("Source '{}' not found", name))?;

        let buffer = self.buffer.clone();
        let s_name = source.name.clone();
        let url = source.url.clone();

        // Spawn the correct network loop based on ConnectionType
        let handle = match source.conn_type {
            ConnectionType::Rest { interval_sec } => {
                let client = self.http_client.clone();
                self.rt.spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_sec));
                    loop {
                        interval.tick().await; 
                        tracing::debug!("Fetching from {}...", s_name);
                        match client.get(&url).await {
                            Ok(bytes) => {
                                if let Ok(text) = String::from_utf8(bytes) {
                                    buffer.lock().unwrap().entry(s_name.clone()).or_insert_with(Vec::new).push(text);
                                }
                            }
                            Err(e) => tracing::error!("Failed to fetch {}: {}", s_name, e),
                        }
                    }
                })
            },
            ConnectionType::WebSocket => {
                self.rt.spawn(async move {
                    tracing::debug!("Connecting WS to {}...", url);
                    match tokio_tungstenite::connect_async(&url).await {
                        Ok((mut ws_stream, _)) => {
                            tracing::info!("Connected to WS: {}", s_name);
                            while let Some(msg) = ws_stream.next().await {
                                if let Ok(tokio_tungstenite::tungstenite::Message::Text(text)) = msg {
                                    buffer.lock().unwrap().entry(s_name.clone()).or_insert_with(Vec::new).push(text.to_string());
                                }
                            }
                            tracing::warn!("WS stream closed for {}", s_name);
                        }
                        Err(e) => tracing::error!("WS connection failed for {}: {}", s_name, e),
                    }
                })
            }
        };

        tasks.insert(name.to_string(), handle);
        tracing::info!("Started source: {}", name);

        Ok(())
    }


    pub fn stop_source(&self, name: &str) {
        if let Some(handle) = self.active_tasks.lock().unwrap().remove(name) {
            handle.abort();
            tracing::info!("Stopped source: {}", name);
        }
    }


    pub fn start_all(&self) {

        let names: Vec<String> = self.sources.lock().unwrap().keys().cloned().collect();
        for name in names {
            let _ = self.start_source(&name);
        }
    }

    pub fn stop_all(&self) {
        let mut tasks_guard = self.active_tasks.lock().unwrap();
        for (_, handle) in tasks_guard.drain() {
            handle.abort();
        }
        tracing::info!("Stopped all background OSINT tasks");
    }


    pub fn poll_data(&self) -> HashMap<String, RecordBatch> {

        let mut buffer_guard = self.buffer.lock().unwrap();
        let raw_data = buffer_guard.clone();
        buffer_guard.clear(); 
        
        let mut parsed_data = HashMap::new();
        let sources = self.sources.lock().unwrap();
        
        for (name, payloads) in raw_data {
            if let Some(source) = sources.get(&name) {
                // Dynamic dispatch
                let batch = source.parser.parse(&payloads);
                parsed_data.insert(name, batch);
            }
        }

        parsed_data
    }

}


impl Drop for Engine {
    fn drop(&mut self) {
        self.stop_all();
    }
}