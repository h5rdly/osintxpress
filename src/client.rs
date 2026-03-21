use std::{future::Future, pin::Pin};

use wreq::Client;
use wreq_util::Emulation;


pub trait HttpClient: Send + Sync {
    fn get<'a>(
        &'a self, 
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;
}


pub struct WreqClient {
    client: Client,
}


impl WreqClient {
    pub fn new() -> Self {
        let client = Client::builder().emulation(Emulation::Safari17_5).build()
            .expect("Failed to build impersonating wreq client");
        Self { client }
    }
}


impl HttpClient for WreqClient {

    fn get<'a>(&'a self, url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        
        Box::pin(async move {
            let resp = self.client.get(url).send().await.map_err(|e| e.to_string())?;
            
            if resp.status().is_success() {
                let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
                Ok(bytes.to_vec())
            } else {
                Err(format!("HTTP Error: {}", resp.status()))
            }
        })
    }
}