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
mod geo;
mod mlt;

use engine::Engine;
use parser::ParserType;

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

impl SourceAdapter {
    pub fn new(pt: ParserType, name: &str, url: &str, alternates: &[&str]) -> Self {
        Self {
            name: name.to_string(),
            default_url: url.to_string(),
            alternate_urls: alternates.iter().map(|&s| s.to_string()).collect(),
            parser_type: pt,
        }
    }
}

#[pyfunction]
pub fn sources() -> Vec<SourceAdapter> {
    let src = |pt: ParserType, name: &str, url: &str| SourceAdapter::new(pt, name, url, &[]);
    let src_with_alt = |pt: ParserType, name: &str, url: &str, alts: &[&str]| SourceAdapter::new(pt, name, url, alts);
    vec![
        src_with_alt(
            ParserType::Binance,   
            "BINANCE",       
            "wss://stream.binance.com:9443/ws/btcusdt@trade", 
            &["wss://stream.binance.us:9443/ws/btcusdt@trade"]
        ),
        src(ParserType::AisStream,     "AIS_STREAM",    "wss://stream.aisstream.io/v0/stream"),
        src_with_alt(
            ParserType::Acled,     
            "ACLED",         
            "https://acleddata.com/api/acled/read", 
            &["https://api.acleddata.com/acled/read"]
        ),
        src(ParserType::NasaEonet,     "NASA_EONET",    "https://eonet.gsfc.nasa.gov/api/v3/events"),
        src(ParserType::OpenSky,       "OPENSKY",       "https://opensky-network.org/api/states/all"),
        src(ParserType::GdeltGeojson,  "GDELT_GEOJSON", "https://api.gdeltproject.org/api/v2/geo/geo?format=geojson"),
        src(
            ParserType::GoogleNewsReuters, 
            "GOOGLE_NEWS_REUTERS", 
            "https://news.google.com/rss/search?q=site%3Areuters.com+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen"
        ),
        src(ParserType::Polymarket,    "POLYMARKET",    "https://gamma-api.polymarket.com/events?limit=50&active=true"),
        src(ParserType::Usgs,          "USGS",          "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"),
        src(ParserType::Nws,           "NWS",           "https://api.weather.gov/alerts/active"),
        src(ParserType::Bbc,           "BBC",           "http://feeds.bbci.co.uk/news/world/rss.xml"),
        src(ParserType::AlJazeera,     "AL_JAZEERA",    "https://www.aljazeera.com/xml/rss/all.xml"),
        src(ParserType::CloudflareRadar, "CLOUDFLARE_RADAR", "https://api.cloudflare.com/client/v4/radar/bgp/leaks/events"),
        src(ParserType::NasaFirms,       "NASA_FIRMS",       "https://firms.modaps.eosdis.nasa.gov/api/country/csv/YOUR_KEY/VIIRS_SNPP_NRT/USA/1"),
        src(ParserType::Urlhaus,         "URLHAUS",          "https://urlhaus-api.abuse.ch/v1/urls/recent/"),
        src(ParserType::Fred,      "FRED",       "https://api.stlouisfed.org/fred/series/observations?series_id=BDI&file_type=json"),
        src(ParserType::Ucdp,      "UCDP",       "https://ucdpapi.pcr.uu.se/api/gedevents/23.1?pagesize=100"),
        src(ParserType::Oref,      "OREF",       "https://www.oref.org.il/WarningMessages/alert/alerts.json"),
        src(ParserType::CoinGecko, "COINGECKO",  "https://api.coingecko.com/api/v3/simple/price?ids=tether,bitcoin&vs_currencies=usd"),
        src(ParserType::OpenMeteo, "OPEN_METEO", "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current_weather=true"),
        src(ParserType::FeodoTracker, "FEODO_TRACKER", "https://feodotracker.abuse.ch/downloads/ipblocklist.json"),
        src(ParserType::RansomwareLive, "RANSOMWARE_LIVE", "https://api.ransomware.live/v2/recentvictims"),
        src(ParserType::NgaWarnings, "NGA_WARNINGS", "https://msi.nga.mil/api/publications/broadcast-warn?output=json"),

    ]
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

    #[pyo3(signature = (adapter, name=None, url=None, poll_interval_sec=60, headers=None))]
    fn add_rest_source(
        &self, adapter: SourceAdapter, name: Option<&str>, url: Option<&str>,
        poll_interval_sec: u64, headers: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let url = url.map(|s| s.to_string()).unwrap_or_else(|| adapter.default_url.to_string());        
        let parser = parser::get_parser(adapter.parser_type); 
        let source_name = name.map(|s| s.to_string()).unwrap_or_else(|| adapter.name.to_lowercase());

        let runner = Box::new(runner::RestRunner { 
            interval_sec: poll_interval_sec, 
            headers,
            client: self.http_client.clone() 
        });

        self.engine.add_source(source_name.clone(), url, runner, parser);
        
        if self.is_running {
            let _ = self.engine.start_source(&source_name);
        }
        Ok(())
    }

    #[pyo3(signature = (adapter, name=None, url=None, init_message=None))]
    fn add_ws_source(
        &self, adapter: SourceAdapter, name: Option<&str>, url: Option<&str>, init_message: Option<String>,
    ) -> PyResult<()> {
        let url = url.map(|s| s.to_string()).unwrap_or_else(|| adapter.default_url.to_string());
        let parser = parser::get_parser(adapter.parser_type); 
        let source_name = name.map(|s| s.to_string()).unwrap_or_else(|| adapter.name.to_lowercase());

        let runner = Box::new(runner::WsRunner { init_message });

        self.engine.add_source(source_name.clone(), url, runner, parser);
        
        if self.is_running {
            let _ = self.engine.start_source(&source_name);
        }
        Ok(())
    }

    #[pyo3(signature = (name, target_channel, tg_api_id, tg_api_hash, tg_session_path))]
    pub fn add_telegram_source(&mut self, name: &str, target_channel: &str, tg_api_id: i32, tg_api_hash: &str, tg_session_path: &str) {
        let parser = crate::parser::get_parser(crate::parser::ParserType::Telegram);
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
    
    m.add_function(wrap_pyfunction!(login_telegram, m)?)?;
    m.add_function(wrap_pyfunction!(sources, m)?)?;

    m.add_function(wrap_pyfunction!(scrape::scrape_article, m)?)?;
    m.add_function(wrap_pyfunction!(geo::fetch_submarine_cables, m)?)?;

    m.add_function(wrap_pyfunction!(arrow_to_mlt, m)?)?;
    m.add_function(wrap_pyfunction!(list_mlt_layers, m)?)?;
    m.add_function(wrap_pyfunction!(mvt_to_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(mlt_to_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(mlt_to_dict, m)?)?; 

    m.add("ENGINE_BUFFER_SIZE", engine::ENGINE_BUFFER_SIZE)?;

    Ok(())
}
