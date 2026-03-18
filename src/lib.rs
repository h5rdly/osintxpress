use pyo3::prelude::*;
use pyo3::types::PyDict;

mod engine;

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

    fn add_rest_source(&self, name: &str, url: &str, poll_interval_sec: u64) {
        tracing::info!(
            "Registered REST source '{}' at {} ({}s interval)", 
            name, url, poll_interval_sec
        );
        // Phase 2: Wire this to the internal configuration state
    }

    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // CRITICAL: We explicitly release the GIL while Rust gathers the data.
        // If draining the buffer takes a few milliseconds, other Python threads 
        // can continue executing.
        let _data = py.allow_threads(|| {
            self.inner.poll_data()
        });

        // For now, just return an empty Python dictionary.
        // Later, we will map `_data` to a pyo3_polars::PyDataFrame here.
        let dict = PyDict::new(py);
        Ok(dict.into_any())
    }
}

/// The Python module initialization. The name must match the `module-name` in pyproject.toml
#[pymodule]
fn _osintxpress(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // This is the magic bridge: it routes all tracing::info! and tracing::debug! 
    // calls from our Rust background threads directly into Python's `logging` module.
    pyo3_log::init();
    
    m.add_class::<OsintEngine>()?;
    Ok(())
}