use std::sync::Arc;
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;

// use arrow::pyarrow::ToPyArrow; 
use pyo3_arrow::PyRecordBatch; 


mod engine;
mod mock_server; 
mod client;
mod parser;

use engine::{Engine, ConnectionType};
use parser::SourceAdapter;


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

    #[pyo3(signature = (adapter, name=None, url=None, poll_interval_sec=60, headers=None, init_message=None))]
    fn add_source(
        &self,
        adapter: SourceAdapter,
        name: Option<&str>, 
        url: Option<&str>,
        poll_interval_sec: u64,
        headers: Option<HashMap<String, String>>,
        init_message: Option<String>,
    ) -> PyResult<()> {

        let pure_adapter: parser::SourceAdapter = adapter.clone().into();

        let url = url.unwrap_or_else(|| pure_adapter.default_url());
        let parser = parser::get_parser(&pure_adapter); 
        let source_name = name.map(|s| s.to_string())
            .unwrap_or_else(|| format!("{:?}", adapter).to_lowercase());

        let conn_type = if pure_adapter.is_ws() {
            ConnectionType::WebSocket { init_message }
        } else {
            ConnectionType::Rest { interval_sec: poll_interval_sec, headers }
        };
        
        self.engine.add_source(source_name, url.to_string(), conn_type, parser);

        Ok(())
    }


    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {

        let raw_batches = py.detach(|| self.engine.poll_data());

        let dict = PyDict::new(py);
        for (name, batch) in raw_batches {
            dict.set_item(name, PyRecordBatch::new(batch).to_arro3(py)?)?;
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