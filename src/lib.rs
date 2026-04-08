use std::sync::Arc;
use std::collections::HashMap;
use tracing_subscriber::fmt::writer::MakeWriterExt;

use grammers_client::Client as TgClient;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyAny};
use pyo3_arrow::{PyRecordBatch}; 

mod engine;
mod mock_server; 
mod client;
mod parser;
mod runner;
mod scrape;
mod telegram;
mod satellites;
mod geo;
mod mlt;

use engine::Engine;
// use parser::ParserType;

#[pyclass(frozen, eq, from_py_object)]
#[derive(Clone, PartialEq, Debug)]
pub struct SourceAdapter {
    pub parser_type: ParserType,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub default_url: String,
    #[pyo3(get)]
    pub alternate_urls: Vec<String>,
}


#[pyclass(from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserType {

    DynamicJson,
    
    Binance,
    AisStream,
    AisHub,
    DigiTraffic,
    Marinesia,
    Telegram,
    Acled,
    OpenSky,
    GdeltGeojson,
    NasaEonet,
    Usgs,
    Urlhaus,
    Fred,
    Oref,
    CoinGecko,
    OpenMeteo,
    GoogleNewsReuters,
    Nws,
    Bbc,
    AlJazeera,
    Polymarket,
    CloudflareRadar,
    NasaFirms,
    Ucdp,
    FeodoTracker,
    RansomwareLive,
    NgaWarnings,
    Unhcr,     
    Celestrak,
}


#[pyclass(subclass)]
pub struct OsintEngine {
    engine: Engine,
    http_client: Arc<dyn client::HttpClient>, 
    is_running: bool,
}


#[pymethods]
impl OsintEngine {
    #[new]
    #[pyo3(signature = (worker_threads=2))]
    fn new(worker_threads: usize) -> Self {
        Self {
            engine: Engine::new(worker_threads),
            http_client: Arc::new(client::WreqClient::new()),
            is_running: false,
        }
    }

    fn start_source(&self, name: &str) -> PyResult<()> {
        self.engine.start_source(name).map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    fn stop_source(&self, name: &str) {
        self.engine.stop_source(name);
    }

    fn start_all(&mut self) {
        self.engine.start_all();
        self.is_running = true;
    }

    fn stop_all(&mut self) {
        self.engine.stop_all();
        self.is_running = false;
    }

    pub fn poll(&self) -> std::collections::HashMap<String, pyo3_arrow::PyRecordBatch> {
        let mut py_dict = std::collections::HashMap::new();
        for (source_name, batch) in self.engine.poll_data() {
            py_dict.insert(source_name, pyo3_arrow::PyRecordBatch::new(batch));
        }
        py_dict
    }

    #[pyo3(signature = (parser_type, name, url, poll_interval_sec=60, headers=None))]
    fn add_rest_source(
        &self, 
        parser_type: ParserType, 
        name: String, 
        url: String,
        poll_interval_sec: u64, 
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        
        let parser = parser::get_parser(parser_type); 
        let runner = Box::new(runner::RestRunner { 
            interval_sec: poll_interval_sec, 
            headers,
            client: self.http_client.clone() 
        });

        self.engine.add_source(name.clone(), url, runner, parser);
        if self.is_running {
            let _ = self.engine.start_source(&name);
        }
        
        Ok(())
    }

    #[pyo3(signature = (parser_type, name, url, init_message=None))]
    fn add_ws_source(
        &self, 
        parser_type: ParserType, 
        name: String, 
        url: String, 
        init_message: Option<String>,
    ) -> PyResult<()> {
        
        let parser = parser::get_parser(parser_type); 
        let runner = Box::new(runner::WsRunner { init_message });
        self.engine.add_source(name.clone(), url, runner, parser);
        if self.is_running {
            let _ = self.engine.start_source(&name);
        }
        
        Ok(())
    }

    #[pyo3(signature = (name, target_channel, tg_api_id, tg_api_hash, tg_session_path))]
    pub fn add_telegram_source(&mut self, name: &str, target_channel: &str, tg_api_id: i32, tg_api_hash: &str, tg_session_path: &str) {
        let parser = crate::parser::get_parser(ParserType::Telegram);
        self.engine.add_telegram_source(
            target_channel.to_string(),
            tg_api_id,
            tg_api_hash.to_string(),
            tg_session_path.to_string(),
            name.to_string(),
            parser
        );
    }

    #[pyo3(signature = (target_channel, limit=5))]
    pub fn fetch_telegram_history(&self, target_channel: &str, limit: usize) {
        self.engine.fetch_telegram_history(target_channel.to_string(), limit);
    }

    #[pyo3(signature = (source_name, payloads))]
    pub fn inject_test_data(&self, source_name: &str, payloads: Vec<String>) {
        self.engine.inject_test_data(source_name, payloads);
    }
}


#[pyfunction]
#[pyo3(signature = (api_id, api_hash, phone, session_path, code_callback))]
fn login_telegram<'py>(
    api_id: i32, 
    api_hash: &str, 
    phone: &str, 
    session_path: &str, 
    code_callback: Bound<'py, PyAny> 
) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let session = Arc::new(SqliteSession::open(session_path).await.unwrap());
        let SenderPool { runner, handle, .. } = SenderPool::new(Arc::clone(&session), api_id);
        let tg_client = TgClient::new(handle);
        let _task = tokio::spawn(runner.run());

        let token = match tg_client.request_login_code(phone, api_hash).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Telegram API Error (request_login_code): {}", e);
                return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()));
            }
        };

        let code: String = code_callback.call0()?.extract()?;

        if let Err(e) = tg_client.sign_in(&token, &code).await {
            tracing::error!("Telegram API Error (sign_in): {}", e);
            return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()));
        }
            
        tracing::info!("Successfully authenticated MTProto session for {}", phone);
        Ok(())
    })
}


// -- GeoJSON 



// -- Aux MLT conversion methods

#[pyfunction]
#[pyo3(signature = (layer_name, batch, lat_col="latitude", lon_col="longitude"))]
fn arrow_to_mlt<'py>(
    py: Python<'py>, layer_name: &str, batch: PyRecordBatch, lat_col: &str, lon_col: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let arrow_batch = batch.into_inner();
    match mlt::MltBridge::encode_from_arrow(layer_name, &arrow_batch, lat_col, lon_col) {
        Ok(mlt_bytes) => Ok(PyBytes::new(py, &mlt_bytes)),
        Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e)),
    }
}

#[pyfunction]
fn list_mlt_layers(mlt_bytes: &[u8]) -> PyResult<Vec<String>> {
    let layers = mlt_core::Parser::default().parse_layers(mlt_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("MLT Parse error: {}", e)))?;
        
    Ok(layers.iter()
        .filter_map(|l| l.as_layer01().map(|l| l.name.to_string()))
        .collect())
}

#[pyfunction]
fn mvt_to_geojson(mvt_bytes: &[u8]) -> PyResult<String> {
    let fc = mlt_core::mvt::mvt_to_feature_collection(mvt_bytes.to_vec())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("MVT Parse error: {}", e)))?;
        
    serde_json::to_string(&fc)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("JSON Serialization error: {}", e)))
}

#[pyfunction]
fn mlt_to_geojson(mlt_bytes: &[u8]) -> PyResult<String> {
    let mut parser = mlt_core::Parser::default();
    let mut layers = parser.parse_layers(mlt_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("MLT Parse error: {}", e)))?;
        
    let mut decoder = mlt_core::Decoder::default();
    let fc = mlt_core::geojson::FeatureCollection::from_layers(&mut layers, &mut decoder)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("GeoJSON error: {}", e)))?;

    serde_json::to_string(&fc)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("JSON Serialization error: {}", e)))
}


// -- Satellite powa

#[pyfunction]
fn mlt_to_dict<'py>(py: Python<'py>, mlt_bytes: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    
    let mut parser = mlt_core::Parser::default();
    let mut layers = parser.parse_layers(mlt_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("MLT Parse error: {}", e)))?;
        
    let mut decoder = mlt_core::Decoder::default();
    let fc = mlt_core::geojson::FeatureCollection::from_layers(&mut layers, &mut decoder)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("GeoJSON error: {}", e)))?;

    let py_dict = pythonize::pythonize(py, &fc)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Pythonize error: {}", e)))?;
        
    Ok(py_dict)
}


#[pyfunction]
pub fn compute_orbits(line1s: Vec<String>, line2s: Vec<String>) -> PyResult<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    let now = chrono::Utc::now().naive_utc();
    let mut lats = Vec::with_capacity(line1s.len());
    let mut lons = Vec::with_capacity(line1s.len());
    let mut alts = Vec::with_capacity(line1s.len());

    for (l1, l2) in line1s.iter().zip(line2s.iter()) {
        // Call your dedicated satellites.rs math module!
        if let Ok(pos) = crate::satellites::compute_satellite_position("SAT", l1, l2, &now) {
            lats.push(pos.latitude);
            lons.push(pos.longitude);
            alts.push(pos.altitude_km);
        } else {
            // If propagation fails (e.g. decayed orbit), push NaNs so Polars can drop them
            lats.push(std::f64::NAN);
            lons.push(std::f64::NAN);
            alts.push(std::f64::NAN);
        }
    }
    Ok((lats, lons, alts))
}

#[pyfunction]
pub fn get_dark_ships(timeout_hours: f64) -> (Vec<i64>, Vec<f64>, Vec<f64>, Vec<String>, Vec<i64>) {
    let now = chrono::Utc::now().timestamp();
    let timeout_sec = (timeout_hours * 3600.0) as i64;

    let mut mmsis = Vec::new();
    let mut lats = Vec::new();
    let mut lons = Vec::new();
    let mut names = Vec::new();
    let mut types = Vec::new();

    // Read the cache from parser.rs
    if let Ok(cache) = parser::AIS_CACHE.read() {
        for (&mmsi, meta) in cache.iter() {
            
            // Criteria 1: Is it a Cargo (70-79) or Tanker (80-89)?
            let is_target = meta.ship_type >= 70 && meta.ship_type <= 89;
            
            // Criteria 2: Was it "Underway using engine" (0)?
            // (If it's anchored or moored, it's normal to turn off AIS)
            let was_underway = meta.last_nav_status == 0;
            
            // Criteria 3: Has it been silent longer than our timeout?
            let is_dark = (now - meta.last_seen) > timeout_sec;

            // Criteria 4: Do we actually have a location for it?
            let has_location = meta.last_lat != 0.0 && meta.last_lon != 0.0;

            if is_target && was_underway && is_dark && has_location {
                mmsis.push(mmsi);
                lats.push(meta.last_lat);
                lons.push(meta.last_lon);
                names.push(meta.name.clone());
                types.push(meta.ship_type);
            }
        }
    }
    
    (mmsis, lats, lons, names, types)
}

#[pymodule]
fn _osintxpress(m: &Bound<'_, PyModule>) -> PyResult<()> {

    if let Ok(log_file) = std::fs::OpenOptions::new().create(true).append(true).open("osint.log")
    {
        // Wrapped in an Arc so it can be shared across Tokio threads
        let file_writer = std::sync::Arc::new(log_file);
        let multi_writer = std::io::stdout.and(file_writer);
        let _ = tracing_subscriber::fmt()
            .with_writer(multi_writer)
            .try_init();
    } else {
        let _ = tracing_subscriber::fmt::try_init(); 
    }

    m.add_class::<OsintEngine>()?;
    m.add_class::<mock_server::MockServer>()?; 
    m.add_class::<SourceAdapter>()?;
    m.add_class::<ParserType>()?;
    
    m.add_function(wrap_pyfunction!(login_telegram, m)?)?;

    m.add_function(wrap_pyfunction!(scrape::scrape_article, m)?)?;
    m.add_function(wrap_pyfunction!(geo::fetch_submarine_cables, m)?)?;

    m.add_function(wrap_pyfunction!(arrow_to_mlt, m)?)?;
    m.add_function(wrap_pyfunction!(list_mlt_layers, m)?)?;
    m.add_function(wrap_pyfunction!(mvt_to_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(mlt_to_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(mlt_to_dict, m)?)?; 

    m.add_function(wrap_pyfunction!(compute_orbits, m)?)?;
    m.add_function(wrap_pyfunction!(get_dark_ships, m)?)?;

    m.add("ENGINE_BUFFER_SIZE", engine::ENGINE_BUFFER_SIZE)?;

    Ok(())
}
