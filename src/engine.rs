use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use arrow::record_batch::RecordBatch;

use crate::parser::SourceParser;
use crate::runner::ConnectionRunner;


pub struct Source {
    pub name: String,
    pub url: String,
    pub runner: Box<dyn ConnectionRunner>, 
    pub parser: Box<dyn SourceParser>,     
}


pub struct Engine {
    rt: Runtime,
    active_tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    sources: Arc<Mutex<HashMap<String, Source>>>,
    buffer: Arc<Mutex<HashMap<String, Vec<String>>>>,
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
        }
    }

    pub fn add_source(
        &self, 
        name: String, 
        url: String, 
        runner: Box<dyn ConnectionRunner>, 
        parser: Box<dyn SourceParser>
    ) {
        self.sources.lock().unwrap().insert(
            name.clone(), 
            Source { name, url, runner, parser }
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
            
            let max_buffer_size = 50;

            // We must enter the Tokio runtime context so the runner can safely call `tokio::spawn`
            let _guard = self.rt.enter();
            
            source.runner.spawn(
                source.name.clone(),
                source.url.clone(),
                self.buffer.clone(),
                max_buffer_size
            )
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
        // Lock, clone, and clear the buffer as fast as possible to unblock writers
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
                        parsed_data.insert(name, batch);
                    }
                    Err(e) => {
                        tracing::error!("Data parsing failed for source '{}': {}", name, e);
                    }
                }
            }
        }

        parsed_data
    }
}


// Ensure background tasks are killed if the Python object is garbage collected
impl Drop for Engine {
    fn drop(&mut self) {
        self.stop_all();
    }
}