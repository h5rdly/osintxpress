use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use futures_util::StreamExt; // read from the WebSocket stream

use arrow::record_batch::RecordBatch;

use crate::client::{HttpClient, ReqwestClient}; // DeboaClient
use crate::{RestAdapter, WsAdapter};
use crate::parser;

#[derive(Clone)]
pub struct RestSource {
    pub name: String,
    pub url: String,
    pub interval_sec: u64,
     pub adapter: RestAdapter, 
}


#[derive(Clone)]
pub struct WsSource {
    pub name: String,
    pub url: String,
    pub adapter: WsAdapter, 
}


pub struct Engine {
    rt: Runtime,
    background_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    rest_sources: Arc<Mutex<Vec<RestSource>>>,
    ws_sources: Arc<Mutex<Vec<WsSource>>>, 
    buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
    http_client: Arc<dyn HttpClient>,
}

impl Engine {

    pub fn new(worker_threads: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        Self {
            rt,
            background_tasks: Arc::new(Mutex::new(Vec::new())),
            rest_sources: Arc::new(Mutex::new(Vec::new())),
            ws_sources: Arc::new(Mutex::new(Vec::new())), 
            buffer: Arc::new(Mutex::new(HashMap::new())),
            http_client: Arc::new(ReqwestClient::new()), 
        }
    }

    pub fn add_rest_source(&self, name: String, url: String, interval_sec: u64, adapter: RestAdapter) {
        self.rest_sources.lock().unwrap().push(RestSource { name, url, interval_sec, adapter });
    }

    pub fn add_ws_source(&self, name: String, url: String, adapter: WsAdapter) {
        self.ws_sources.lock().unwrap().push(WsSource { name, url, adapter });
    }


     pub fn stop(&self) {

        let mut tasks_guard = self.background_tasks.lock().unwrap();
        for handle in tasks_guard.drain(..) {
            handle.abort();
        }
        tracing::info!("Stopped all background OSINT tasks");
    }


    pub fn start(&self) {

        let rest_sources = self.rest_sources.lock().unwrap().clone();
        let ws_sources = self.ws_sources.lock().unwrap().clone();
        let mut tasks_guard = self.background_tasks.lock().unwrap();

        if !tasks_guard.is_empty() {
            tracing::warn!("OsintEngine is already running!");
            return;
        }

        tracing::info!("Starting background OSINT ingestion tasks...");

        // Spawn REST polling tasks
        for source in rest_sources {
            let client = self.http_client.clone();
            let buffer = self.buffer.clone();
            let name = source.name.clone();
            let url = source.url.clone();
            
            let handle = self.rt.spawn(async move {

                let mut interval = tokio::time::interval(std::time::Duration::from_secs(source.interval_sec));
                loop {
                    interval.tick().await; 
                    tracing::debug!("Fetching from {}...", name);
                    match client.get(&url).await {
                        Ok(bytes) => {
                            if let Ok(text) = String::from_utf8(bytes) {
                                buffer.lock().unwrap().entry(name.clone()).or_insert_with(Vec::new).push(text);
                            }
                        }
                        Err(e) => tracing::error!("Failed to fetch {}: {}", name, e),
                    }
                }
            });
            tasks_guard.push(handle);
        }

        // Spawn WebSocket streaming tasks
        for source in ws_sources {
            let buffer = self.buffer.clone();
            let name = source.name.clone();
            let url = source.url.clone();
            
            let handle = self.rt.spawn(async move {
                tracing::debug!("Connecting WS to {}...", url);
                match tokio_tungstenite::connect_async(&url).await {
                    Ok((mut ws_stream, _)) => {
                        tracing::info!("Connected to WS: {}", name);
                        // Await the firehose of messages
                        while let Some(msg) = ws_stream.next().await {
                            if let Ok(tokio_tungstenite::tungstenite::Message::Text(text)) = msg {
                                buffer.lock().unwrap()
                                    .entry(name.clone())
                                    .or_insert_with(Vec::new)
                                    // Convert Tungstenite's Utf8Bytes to a standard String
                                    .push(text.to_string());
                            }
                        }
                        tracing::warn!("WS stream closed for {}", name);
                    }
                    Err(e) => tracing::error!("WS connection failed for {}: {}", name, e),
                }
            });
            tasks_guard.push(handle);
        }
    }


    pub fn poll_data(&self) -> HashMap<String, RecordBatch> {

        let mut buffer_guard = self.buffer.lock().unwrap();
        let raw_data = buffer_guard.clone();
        buffer_guard.clear(); 
        
        let mut parsed_data = HashMap::new();
        
        let rest_sources = self.rest_sources.lock().unwrap();
        for source in rest_sources.iter() {
            if let Some(payloads) = raw_data.get(&source.name) {
                let batch = parser::parse_rest_data(&source.adapter, payloads);
                parsed_data.insert(source.name.clone(), batch);
            }
        }
        
        let ws_sources = self.ws_sources.lock().unwrap();
        for source in ws_sources.iter() {
            if let Some(payloads) = raw_data.get(&source.name) {
                let batch = parser::parse_ws_data(&source.adapter, payloads);
                parsed_data.insert(source.name.clone(), batch);
            }
        }

        parsed_data
    }
}

