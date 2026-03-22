use std::sync::Arc;
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use arrow::pyarrow::ToPyArrow; 

mod engine;
mod mock_server; 
mod client;
mod parser;

use engine::{Engine, ConnectionType};


#[allow(non_camel_case_types)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Clone, PartialEq, Debug)]
pub enum SourceAdapter {
    // REST sources
    ACLED,
    GDELT_GEOJSON,
    OPENSKY,
    REUTERS,

    // WebSocket sources
    AIS_STREAM,
}

impl SourceAdapter {
    pub fn default_url(&self) -> &'static str {
        match self {
            SourceAdapter::ACLED => "https://acleddata.com/api/acled/read",
            SourceAdapter::OPENSKY => "https://opensky-network.org/api/states/all",
            SourceAdapter::GDELT_GEOJSON => "https://api.gdeltproject.org/api/v2/geo/geo?format=geojson",
            SourceAdapter::REUTERS => "https://www.reutersagency.com/feed/",
            SourceAdapter::AIS_STREAM => "wss://stream.aisstream.io/v0/stream",
        }
    }
}


#[pyclass]
pub struct OsintEngine {
    engine: Engine,
}


#[pymethods]
impl OsintEngine {

    #[new]
    #[pyo3(signature = (worker_threads=2))]
    fn new(worker_threads: usize) -> Self {
        let http_client = Arc::new(client::WreqClient::new());
        Self {
            engine: Engine::new(worker_threads, http_client),
        }
    }

    fn start_source(&self, name: &str) -> PyResult<()> {
        self.engine.start_source(name).map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    fn stop_source(&self, name: &str) {
        self.engine.stop_source(name);
    }

    fn start_all(&self) {
        self.engine.start_all();
    }

    fn stop_all(&self) {
        self.engine.stop_all();
    }

    #[pyo3(signature = (name, source_type, adapter, url=None, poll_interval_sec=60, headers=None, init_message=None))]
    fn add_source(&self, name: &str, source_type: &str, adapter: SourceAdapter, url: Option<&str>, 
        poll_interval_sec: u64, headers: Option<HashMap<String, String>>, init_message: Option<String>,
    ) -> PyResult<()> {
        
        let url = url.unwrap_or_else(|| adapter.default_url());

        let conn_type = match source_type.to_lowercase().as_str() {
            "ws" | "websocket" => ConnectionType::WebSocket {init_message},
            "rest" | "http" => {
                tracing::info!("Registered REST source '{}' at {} ({}s interval) using {:?}", name, url, poll_interval_sec, adapter);
                ConnectionType::Rest { interval_sec: poll_interval_sec, headers }
            },
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "source_type must be either 'rest' or 'ws'"
                ));
            }
        };
        
        let parser = parser::get_parser(&adapter); 
        self.engine.add_source(name.to_string(), url.to_string(), conn_type, parser);

        Ok(())
    }


    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {

        let raw_batches = py.detach(|| self.engine.poll_data());

        let dict = PyDict::new(py);
        for (name, batch) in raw_batches {
            let pyarrow_obj = batch.to_pyarrow(py)?; 
            dict.set_item(name, pyarrow_obj)?;
        }

        Ok(dict)
    }
}


#[pymodule]
fn _osintxpress(m: &Bound<'_, PyModule>) -> PyResult<()> {

    let _ = tracing_subscriber::fmt::try_init();

    m.add_class::<OsintEngine>()?;
    m.add_class::<mock_server::MockServer>()?; 
    m.add_class::<SourceAdapter>()?;

    Ok(())
}