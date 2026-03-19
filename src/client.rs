use async_trait::async_trait;

use reqwest::Client;


#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String>;
}


pub struct ReqwestClient {
    client: Client,
}

impl ReqwestClient {
    pub fn new() -> Self {
        Self {
            // We configure connection pooling and a user agent here
            client: Client::builder()
                .user_agent("osintxpress/0.1")
                .build()
                .unwrap(),
        }
    }
}


#[async_trait]
impl HttpClient for ReqwestClient {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        // Fetch the data and safely map reqwest errors to standard strings
        let resp = self.client.get(url).send().await.map_err(|e| e.to_string())?;
        
        if !resp.status().is_success() {
            return Err(format!("HTTP Error: {}", resp.status()));
        }
        
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok(bytes.to_vec())
    }
}