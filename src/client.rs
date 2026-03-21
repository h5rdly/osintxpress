use std::{future::Future, pin::Pin, str::FromStr, time::Duration};
use std::collections::HashMap;

use wreq::{Client, header::{HeaderName, HeaderValue}};
use wreq_util::Emulation;


pub trait HttpClient: Send + Sync {
    fn get<'a>(&'a self, url: &'a str, headers: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;
}

const NETWORK_TIMEOUT: u64 = 15;

pub struct WreqClient {
    client: Client,
}


impl WreqClient {
    pub fn new() -> Self {
        let client = Client::builder().emulation(Emulation::Safari17_5).timeout(Duration::from_secs(NETWORK_TIMEOUT))
        .build().expect("Failed to build impersonating wreq client");
        Self { client }
    }
}


impl HttpClient for WreqClient {

    fn get<'a>(&'a self, url: &'a str, headers: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {

        Box::pin(async move {
            let mut req = self.client.get(url);
            if let Some(custom_headers) = headers {
                for (k, v) in custom_headers {
                    if let (Ok(name), Ok(val)) = (HeaderName::from_str(&k), HeaderValue::from_str(&v)) {
                        req = req.header(name, val);
                    }
                }
            }
            
            let resp = req.send().await.map_err(|e| e.to_string())?;
            
            if resp.status().is_success() {
                let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
                Ok(bytes.to_vec())
            } else {
                Err(format!("HTTP Error: {}", resp.status()))
            }
        })
    }
}