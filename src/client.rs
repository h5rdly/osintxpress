use async_trait::async_trait;

use reqwest::Client as Reqwest;
// use deboa::Deboa;


#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String>;
}


// -- Reqwest implementaion 

pub struct ReqwestClient {
    client: Reqwest,
}

impl ReqwestClient {

    pub fn new() -> Self {
        Self {
            client: Reqwest::builder().user_agent("osintxpress/0.1").build().unwrap(),
        }
    }
}

#[async_trait]
impl HttpClient for ReqwestClient {

    async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        let resp = self.client.get(url).send().await.map_err(|e| e.to_string())?;
        
        if !resp.status().is_success() {
            return Err(format!("HTTP Error: {}", resp.status()));
        }
        
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok(bytes.to_vec())
    }
}

// -- Deboa implementaion 

// pub struct DeboaClient {
//     client: Mutex<Deboa>,
// }

// impl DeboaClient {

//     pub fn new() -> Self {
//         Self { client: Mutex::new(Deboa::new()),}
//     }
// }

// #[async_trait]
// impl HttpClient for DeboaClient {

//     async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        
//         let mut client_guard = self.client.lock().await;
        
//         // Execute using the mutable guard
//         let response = client_guard.execute(url).await.map_err(|e| e.to_string())?;
//         if !response.status().is_success() {
//             return Err(format!("HTTP Error: {}", response.status()));
//         }
//         let bytes = response.text().await.map(|t| t.into_bytes()).map_err(|e| e.to_string())?;
        
//         Ok(bytes)
//     }
// }