use std::sync::Arc;
use std::collections::HashMap;

use grammers_client::Client as TgClient;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyAny};

use pyo3_arrow::PyRecordBatch; 

mod engine;
mod mock_server; 
mod client;
mod parser;
mod runner;

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
        
        src_with_alt(
            ParserType::Binance,   
            "BINANCE",       
            "wss://stream.binance.com:9443/ws/btcusdt@trade", 
            &["wss://stream.binance.us:9443/ws/btcusdt@trade"]
        ),
        src(ParserType::AisStream,     "AIS_STREAM",    "wss://stream.aisstream.io/v0/stream"),
    ]
}



#[pyclass]
pub struct OsintEngine {
    engine: Engine,
    http_client: Arc<dyn client::HttpClient>, 
}


#[pymethods]
impl OsintEngine {
    #[new]
    #[pyo3(signature = (worker_threads=2))]
    fn new(worker_threads: usize) -> Self {
        Self {
            engine: Engine::new(worker_threads),
            http_client: Arc::new(client::WreqClient::new()),
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

    #[pyo3(signature = (adapter, name=None, url=None, poll_interval_sec=60, headers=None))]
    fn add_rest_source(
        &self,
        adapter: SourceAdapter,
        name: Option<&str>, 
        url: Option<&str>,
        poll_interval_sec: u64,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<()> {

        let url = url.map(|s| s.to_string()).unwrap_or_else(|| adapter.default_url.to_string());        let parser = parser::get_parser(adapter.parser_type); 
        let source_name = name.map(|s| s.to_string()).unwrap_or_else(|| adapter.name.to_lowercase());

        let runner = Box::new(runner::RestRunner { 
            interval_sec: poll_interval_sec, 
            headers,
            client: self.http_client.clone() 
        });

        self.engine.add_source(source_name, url, runner, parser);
        Ok(())
    }


    #[pyo3(signature = (adapter, name=None, url=None, init_message=None))]
    fn add_ws_source(
        &self,
        adapter: SourceAdapter,
        name: Option<&str>, 
        url: Option<&str>,
        init_message: Option<String>,
    ) -> PyResult<()> {

        let url = url.map(|s| s.to_string()).unwrap_or_else(|| adapter.default_url.to_string());
        let parser = parser::get_parser(adapter.parser_type); 
        let source_name = name.map(|s| s.to_string()).unwrap_or_else(|| adapter.name.to_lowercase());

        let runner = Box::new(runner::WsRunner { init_message });

        self.engine.add_source(source_name, url, runner, parser);
        Ok(())
    }


    #[pyo3(signature = (target_channel, tg_api_id, tg_session_path=None, name=None))]
    fn add_telegram_source(
        &self,
        target_channel: &str,
        tg_api_id: i32,
        tg_session_path: Option<String>,
        name: Option<&str>, 
    ) -> PyResult<()> {

        let parser = parser::get_parser(parser::ParserType::Telegram);
        let source_name = name.map(|s| s.to_string()).unwrap_or_else(|| "telegram".to_string());

        let runner = Box::new(runner::TelegramRunner {
            api_id: tg_api_id,
            session_path: tg_session_path.unwrap_or_else(|| "osint.session".to_string()),
        });
        
        self.engine.add_source(source_name, target_channel.to_string(), runner, parser);
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


#[pyfunction]
#[pyo3(signature = (api_id, api_hash, phone, session_path, code_callback))]
fn login_telegram<'py>(
    api_id: i32, 
    api_hash: &str, 
    phone: &str, 
    session_path: &str, 
    code_callback: Bound<'py, PyAny> 
) -> PyResult<()> {

    // A temporary single-threaded runtime for the login, to avoid passing python objects to threads
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let session = Arc::new(SqliteSession::open(session_path).await.unwrap());
        let pool = SenderPool::new(Arc::clone(&session), api_id);
        let tg_client = TgClient::new(pool.handle);
        let _task = tokio::spawn(pool.runner.run());

        let token = tg_client.request_login_code(phone, api_hash).await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        // On the main thread, we don't need `with_gil` or `allow_threads`
        let code: String = code_callback.call0()?.extract()?;

        tg_client.sign_in(&token, &code).await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            
        Ok(())
    })
}


#[pymodule]
fn _osintxpress(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt::try_init();

    m.add_class::<OsintEngine>()?;
    m.add_class::<mock_server::MockServer>()?; 
    m.add_class::<SourceAdapter>()?;
    
    m.add_function(wrap_pyfunction!(login_telegram, m)?)?;
    m.add_function(wrap_pyfunction!(sources, m)?)?;

    Ok(())
}