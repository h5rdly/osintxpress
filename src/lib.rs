
use pyo3::prelude::*;
use pyo3::types::PyDict;

use arrow::pyarrow::ToPyArrow; 

mod engine;
mod mock_server; 
mod client;
mod parser;


#[allow(non_camel_case_types)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Clone, PartialEq, Debug)]
pub enum RestAdapter {
    ACLED,
    GDELT_GEOJSON,
    OPENSKY,
    RSS,
}

#[allow(non_camel_case_types)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Clone, PartialEq, Debug)]
pub enum WsAdapter {
    AIS_STREAM,
}


#[pyclass]
pub struct OsintEngine {
    inner: engine::Engine,
}

#[pymethods]
impl OsintEngine {

    #[new]
    #[pyo3(signature = (worker_threads=2))]
    fn new(worker_threads: usize) -> Self {
        Self {
            inner: engine::Engine::new(worker_threads),
        }
    }


    fn start(&self) {
        self.inner.start();
    }

    fn stop(&self) {
        self.inner.stop();
    }


    #[pyo3(signature = (name, url, poll_interval_sec, adapter))]
    fn add_rest_source(&self, name: &str, url: &str, poll_interval_sec: u64, adapter: RestAdapter) {
        
        tracing::info!(
            "Registered REST source '{}' at {} ({}s interval) using {:?}", 
            name, url, poll_interval_sec, adapter
        );
        self.inner.add_rest_source(name.to_string(), url.to_string(), poll_interval_sec, adapter);
    }


    #[pyo3(signature = (name, url, adapter))]
    fn add_ws_source(&self, name: &str, url: &str, adapter: WsAdapter) {

        tracing::info!("Registered WS source '{}' at {} using {:?}", name, url, adapter);
        self.inner.add_ws_source(name.to_string(), url.to_string(), adapter);
    }


    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {

        // Fetch the Rust Arrow RecordBatches without the GIL
        let raw_batches = py.detach(|| {self.inner.poll_data()});

        // Acquire the GIL and export to Python
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
    _ = pyo3_log::try_init();  
    
    m.add_class::<OsintEngine>()?;
    m.add_class::<mock_server::MockServer>()?; 
    
    m.add_class::<RestAdapter>()?;
    m.add_class::<WsAdapter>()?;

    Ok(())
}