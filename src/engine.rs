use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::sync::mpsc;

use arrow::record_batch::RecordBatch;

use crate::parser::SourceParser;
use crate::runner::{ConnectionRunner, TelegramMultiplexer, TgCommand};

pub const ENGINE_BUFFER_SIZE: usize = 1000;

pub struct Source {
    pub name: String,
    pub url: String,
    pub runner: Option<Box<dyn ConnectionRunner>>,
    pub parser: Box<dyn SourceParser>,     
}

pub struct Engine {
    rt: Runtime,
    active_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    sources: Arc<Mutex<HashMap<String, Source>>>,
    buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,

    telegram_config: Option<(i32, String, String)>, 
    telegram_channels: Vec<(String, String)>,
    telegram_task: Option<JoinHandle<()>>,
    telegram_cmd_tx: Option<mpsc::Sender<TgCommand>>, 
    max_buffer_size: usize,
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
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            sources: Arc::new(Mutex::new(HashMap::new())),
            buffer: Arc::new(Mutex::new(HashMap::new())),
            telegram_config: None,
            telegram_channels: Vec::new(),
            telegram_task: None,
            telegram_cmd_tx: None,
            max_buffer_size: ENGINE_BUFFER_SIZE,
        }
    }


    pub fn add_source(
        &self, name: String, url: String, runner: Box<dyn ConnectionRunner>, parser: Box<dyn SourceParser>
    ) {
        self.sources.lock().unwrap().insert(
            name.clone(), 
            Source { name, url, runner: Some(runner), parser }
        );
    }


    pub fn start_source(&self, name: &str) -> Result<(), String> {
        let mut tasks = self.active_tasks.lock().unwrap();
        
        if tasks.contains_key(name) {
            tracing::warn!("Source '{}' is already running.", name);
            return Ok(());
        }

        let handle = {
            let sources_guard = self.sources.lock().unwrap();
            let source = sources_guard.get(name).ok_or_else(|| format!("Source '{}' not found", name))?;
            
            if let Some(runner) = &source.runner {
                let _guard = self.rt.enter();
                Some(runner.spawn(
                    source.name.clone(),
                    source.url.clone(),
                    self.buffer.clone(),
                    self.max_buffer_size
                ))
            } else {
                None
            }
        };

        if let Some(h) = handle {
            tasks.insert(name.to_string(), h);
            tracing::info!("Started source: {}", name);
        }

        Ok(())
    }


    pub fn stop_source(&self, name: &str) {
        if let Some(handle) = self.active_tasks.lock().unwrap().remove(name) {
            handle.abort();
            tracing::info!("Stopped source: {}", name);
        }
    }


    pub fn add_telegram_source(
        &mut self, target_url: String, tg_api_id: i32, tg_api_hash: String, tg_session_path: String, name: String, parser: Box<dyn SourceParser>
    ) {

        if self.telegram_config.is_none() {
            self.telegram_config = Some((tg_api_id, tg_api_hash, tg_session_path));
        }
        self.telegram_channels.push((name.clone(), target_url.clone()));
        
        self.sources.lock().unwrap().insert(
            name.clone(),
            Source { name, url: target_url, runner: None, parser }
        );

        if self.telegram_task.is_some() {
            self.restart_telegram();
        }
    }


    pub fn restart_telegram(&mut self) {

        if let Some(handle) = self.telegram_task.take() {
            handle.abort();
            tracing::info!("Restarting Telegram Multiplexer with new channels...");
        }

        if let Some((api_id, ref _api_hash, ref session_path)) = self.telegram_config {
            if !self.telegram_channels.is_empty() {
                let (tx, rx) = mpsc::channel(32); 
                self.telegram_cmd_tx = Some(tx);
                
                let runner = TelegramMultiplexer {
                    api_id,
                    session_path: session_path.clone(),
                    channels: self.telegram_channels.clone(), 
                };
                
                let _guard = self.rt.enter();
                let handle = runner.spawn(
                    self.buffer.clone(),
                    self.max_buffer_size,
                    rx 
                );
                self.telegram_task = Some(handle);
            }
        }
    }
    

    pub fn fetch_telegram_history(&self, target_channel: String, limit: usize) {
        if let Some(tx) = &self.telegram_cmd_tx {
            let tx_clone = tx.clone();
            let _guard = self.rt.enter();
            tokio::spawn(async move {
                let _ = tx_clone.send(TgCommand::FetchHistory { target_channel, limit }).await;
            });
        } else {
            tracing::warn!("Cannot fetch history: Telegram multiplexer is not running!");
        }
    }

    pub fn start_all(&mut self) {
        let names: Vec<String> = self.sources.lock().unwrap().keys().cloned().collect();
        for name in names {
            let _ = self.start_source(&name);
        }
        self.restart_telegram();
    }

    pub fn stop_all(&mut self) {
        let mut tasks_guard = self.active_tasks.lock().unwrap();
        for (_, handle) in tasks_guard.drain() {
            handle.abort();
        }
        
        if let Some(handle) = self.telegram_task.take() {
            handle.abort();
        }
        
        tracing::info!("Stopped all background OSINT tasks");
    }

    pub fn poll_data(&self) -> HashMap<String, RecordBatch> {
        let raw_data = {
            let mut buffer_guard = self.buffer.lock().unwrap();
            let data = buffer_guard.clone();
            buffer_guard.clear(); 
            data
        };
        
        let mut parsed_data = HashMap::new();
        let sources = self.sources.lock().unwrap();
        
        for (name, payloads) in raw_data {            
            if let Some(source) = sources.get(&name) {
                match source.parser.parse(&payloads) {
                    Ok(batch) => {
                        // println!("✅ [RUST PARSE SUCCESS] Source: '{}' | Rows: {}", name, batch.num_rows());
                        parsed_data.insert(name, batch);
                    }
                    Err(e) => {
                        println!("❌ [RUST PARSE FAILED] Source: '{}' | Error: {}", name, e);
                    }
                }
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