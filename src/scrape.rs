use pyo3::prelude::*;
use scraper::{Html, Selector};
use std::collections::HashMap;

use crate::client::{HttpClient, WreqClient};


#[pyfunction]
pub fn scrape_article(url: &str) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let client = WreqClient::new();
        let mut headers = HashMap::new();
        headers.insert(
            "User-Agent".to_string(), 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36".to_string()
        );
        
        let bytes = client.get(url, Some(headers)).await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to fetch: {}", e)))?;
            
        let html_str = String::from_utf8_lossy(&bytes);
        let document = Html::parse_document(&html_str);
        
        // Target all paragraph tags
        let selector = Selector::parse("p").unwrap();
        
        let mut article_text = String::new();
        for element in document.select(&selector) {
            let text: String = element.text().collect::<Vec<_>>().join("");
            let text = text.trim();
            
            // Filter out tiny UI elements/buttons that use <p> tags
            if text.len() > 30 {
                article_text.push_str(text);
                article_text.push_str("\n\n");
            }
        }
        
        if article_text.is_empty() {
            Ok("Could not extract article text. The site might be using heavy JavaScript rendering or an anti-bot wall.".to_string())
        } else {
            Ok(article_text.trim_end().to_string())
        }
    })
}