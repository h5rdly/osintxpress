use std::sync::Arc;

use serde_json::Value;
use quick_xml::events::Event;
use quick_xml::Reader;

use arrow::array::{Float64Builder, Int32Builder, Int64Builder, StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserType {
    Binance,
    AisStream,
    
    Telegram,

    Acled,
    OpenSky,
    GdeltGeojson,
    GoogleNewsReuters,
    NasaEonet,
    Polymarket,
    Usgs,
    Nws,
    Bbc,
    AlJazeera,

    CloudflareRadar,
    NasaFirms,
    Urlhaus,
    Fred,
    Ucdp,
    Oref,
    CoinGecko,
    OpenMeteo,
}


pub fn get_parser(parser_type: ParserType) -> Box<dyn SourceParser> {
    match parser_type {
        ParserType::Binance       => Box::new(BinanceParser),
        ParserType::AisStream     => Box::new(AisStreamParser),
        
        ParserType::Telegram      => Box::new(TelegramParser),

        ParserType::Acled         => Box::new(AcledParser),
        ParserType::OpenSky       => Box::new(OpenSkyParser),
        
        ParserType::GdeltGeojson  => Box::new(GdeltParser), 
        ParserType::NasaEonet     => Box::new(EonetParser),
        
        ParserType::Polymarket    => Box::new(PolymarketParser),
        ParserType::Usgs          => Box::new(UsgsParser),
        
        ParserType::GoogleNewsReuters => Box::new(RssParser),
        ParserType::Nws           => Box::new(RssParser),
        ParserType::Bbc           => Box::new(RssParser),
        ParserType::AlJazeera     => Box::new(RssParser),

        ParserType::CloudflareRadar => Box::new(CloudflareRadarParser),
        ParserType::NasaFirms       => Box::new(NasaFirmsParser),
        ParserType::Urlhaus         => Box::new(UrlhausParser),
        ParserType::Fred      => Box::new(FredParser),
        ParserType::Ucdp      => Box::new(UcdpParser),
        ParserType::Oref      => Box::new(OrefParser),
        ParserType::CoinGecko => Box::new(CoinGeckoParser),
        ParserType::OpenMeteo => Box::new(OpenMeteoParser),
    }

}


// -- Parser Trait and implementations

pub trait SourceParser: Send + Sync {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String>;
}


pub struct AisStreamParser;
impl SourceParser for AisStreamParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut mmsi_builder = Int64Builder::new();
        let mut name_builder = StringBuilder::new();
        let mut lat_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut speed_builder = Float64Builder::new();
        let mut heading_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                // AisStream puts the identity and location fields in "MetaData"
                let meta = json.get("MetaData");
                
                mmsi_builder.append_option(meta.and_then(|m| m.get("MMSI")).and_then(|v| v.as_i64()));
                name_builder.append_option(meta.and_then(|m| m.get("ShipName")).and_then(|v| v.as_str()));
                lat_builder.append_option(meta.and_then(|m| m.get("latitude")).and_then(|v| v.as_f64()));
                lon_builder.append_option(meta.and_then(|m| m.get("longitude")).and_then(|v| v.as_f64()));

                let report = json.get("Message").and_then(|m| m.get("PositionReport"));
                speed_builder.append_option(report.and_then(|r| r.get("Sog")).and_then(|v| v.as_f64()));
                heading_builder.append_option(report.and_then(|r| r.get("TrueHeading")).and_then(|v| v.as_f64()));
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("mmsi", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("speed", DataType::Float64, true),
            Field::new("heading", DataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(mmsi_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(lat_builder.finish()),
                Arc::new(lon_builder.finish()),
                Arc::new(speed_builder.finish()),
                Arc::new(heading_builder.finish()),
            ],
        ).map_err(|e| format!("Failed to build AIS RecordBatch: {}", e))
    }
}


pub struct AcledParser;
impl SourceParser for AcledParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut id_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut lat_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut fat_builder = Int32Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                    for item in data {
                        id_builder.append_option(item.get("event_id_cnty").and_then(|v| v.as_str()));
                        type_builder.append_option(item.get("event_type").and_then(|v| v.as_str()));

                        // ACLED returns coordinates and fatalities as strings
                        if let Some(lat_str) = item.get("latitude").and_then(|v| v.as_str()) {
                            lat_builder.append_option(lat_str.parse::<f64>().ok());
                        } else {
                            lat_builder.append_null();
                        }

                        if let Some(lon_str) = item.get("longitude").and_then(|v| v.as_str()) {
                            lon_builder.append_option(lon_str.parse::<f64>().ok());
                        } else {
                            lon_builder.append_null();
                        }

                        if let Some(fat_str) = item.get("fatalities").and_then(|v| v.as_str()) {
                            fat_builder.append_option(fat_str.parse::<i32>().ok());
                        } else {
                            fat_builder.append_null();
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id_cnty", DataType::Utf8, true),
            Field::new("event_type", DataType::Utf8, true),
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("fatalities", DataType::Int32, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(lat_builder.finish()),
                Arc::new(lon_builder.finish()),
                Arc::new(fat_builder.finish()),
            ],
        ).map_err(|e| format!("Failed to build ACLED RecordBatch: {}", e))
    }
}


pub struct OpenSkyParser;
impl SourceParser for OpenSkyParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut icao24_builder = StringBuilder::new();
        let mut callsign_builder = StringBuilder::new();
        let mut origin_country_builder = StringBuilder::new();
        let mut time_position_builder = Int64Builder::new();
        let mut last_contact_builder = Int64Builder::new();
        let mut longitude_builder = Float64Builder::new();
        let mut latitude_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(states_array) = val.get("states").and_then(|s| s.as_array()) {
                    for state in states_array {
                        if let Some(arr) = state.as_array() {
                            icao24_builder.append_option(arr.get(0).and_then(|v| v.as_str()));
                            callsign_builder.append_option(arr.get(1).and_then(|v| v.as_str()).map(|s| s.trim()));
                            origin_country_builder.append_option(arr.get(2).and_then(|v| v.as_str()));
                            time_position_builder.append_option(arr.get(3).and_then(|v| v.as_i64()));
                            last_contact_builder.append_option(arr.get(4).and_then(|v| v.as_i64()));
                            longitude_builder.append_option(arr.get(5).and_then(|v| v.as_f64()));
                            latitude_builder.append_option(arr.get(6).and_then(|v| v.as_f64()));
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("icao24", DataType::Utf8, true),
            Field::new("callsign", DataType::Utf8, true),
            Field::new("origin_country", DataType::Utf8, true),
            Field::new("time_position", DataType::Int64, true),
            Field::new("last_contact", DataType::Int64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("latitude", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(icao24_builder.finish()),
            Arc::new(callsign_builder.finish()),
            Arc::new(origin_country_builder.finish()),
            Arc::new(time_position_builder.finish()),
            Arc::new(last_contact_builder.finish()),
            Arc::new(longitude_builder.finish()),
            Arc::new(latitude_builder.finish()),
        ]).map_err(|e| format!("Failed to build OpenSky batch: {}", e))
    }
}


pub struct GdeltParser;
impl SourceParser for GdeltParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut name_builder = StringBuilder::new();
        let mut url_builder = StringBuilder::new();
        let mut longitude_builder = Float64Builder::new();
        let mut latitude_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(features) = val.get("features").and_then(|f| f.as_array()) {
                    for feature in features {
                        if let Some(props) = feature.get("properties") {
                            name_builder.append_option(props.get("name").and_then(|v| v.as_str()));
                            url_builder.append_option(props.get("url").and_then(|v| v.as_str()));
                        } else {
                            name_builder.append_null();
                            url_builder.append_null();
                        }

                        if let Some(coords) = feature.get("geometry").and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                            longitude_builder.append_option(coords.get(0).and_then(|v| v.as_f64()));
                            latitude_builder.append_option(coords.get(1).and_then(|v| v.as_f64()));
                        } else {
                            longitude_builder.append_null();
                            latitude_builder.append_null();
                        }
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("latitude", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(name_builder.finish()),
            Arc::new(url_builder.finish()),
            Arc::new(longitude_builder.finish()),
            Arc::new(latitude_builder.finish()),
        ]).map_err(|e| format!("Failed to build GDELT batch: {}", e))
    }
}


pub struct UsgsParser;
impl SourceParser for UsgsParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut place_builder = StringBuilder::new();
        let mut mag_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut lat_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(features) = val.get("features").and_then(|f| f.as_array()) {
                    for feature in features {
                        if let Some(props) = feature.get("properties") {
                            place_builder.append_option(props.get("place").and_then(|v| v.as_str()));
                            mag_builder.append_option(props.get("mag").and_then(|v| v.as_f64()));
                        } else {
                            place_builder.append_null();
                            mag_builder.append_null();
                        }

                        if let Some(coords) = feature.get("geometry").and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                            lon_builder.append_option(coords.get(0).and_then(|v| v.as_f64()));
                            lat_builder.append_option(coords.get(1).and_then(|v| v.as_f64()));
                        } else {
                            lon_builder.append_null();
                            lat_builder.append_null();
                        }
                    }
                }
            }
        }
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("place", DataType::Utf8, true),
            Field::new("magnitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("latitude", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(place_builder.finish()),
            Arc::new(mag_builder.finish()),
            Arc::new(lon_builder.finish()),
            Arc::new(lat_builder.finish()),
        ]).map_err(|e| format!("Failed to build USGS batch: {}", e))
    }
}


pub struct BinanceParser;
impl SourceParser for BinanceParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut price_builder = Float64Builder::new();
        let mut qty_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                // Binance returns price and quantity as strings in the JSON
                if let Some(p_str) = val.get("p").and_then(|v| v.as_str()) {
                    price_builder.append_option(p_str.parse::<f64>().ok());
                } else {
                    price_builder.append_null();
                }
                
                if let Some(q_str) = val.get("q").and_then(|v| v.as_str()) {
                    qty_builder.append_option(q_str.parse::<f64>().ok());
                } else {
                    qty_builder.append_null();
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("quantity", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(price_builder.finish()),
            Arc::new(qty_builder.finish()),
        ]).map_err(|e| format!("Failed to build Binance batch: {}", e))
    }
}


pub struct EonetParser;
impl SourceParser for EonetParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut title_builder = StringBuilder::new();
        let mut cat_builder = StringBuilder::new();
        let mut lon_builder = Float64Builder::new();
        let mut lat_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload) {
                if let Some(events) = val.get("events").and_then(|e| e.as_array()) {
                    for event in events {
                        title_builder.append_option(event.get("title").and_then(|v| v.as_str()));
                        
                        if let Some(cats) = event.get("categories").and_then(|c| c.as_array()) {
                            cat_builder.append_option(cats.get(0).and_then(|c| c.get("title")).and_then(|v| v.as_str()));
                        } else {
                            cat_builder.append_null();
                        }

                        if let Some(geoms) = event.get("geometry").and_then(|g| g.as_array()) {
                            if let Some(coords) = geoms.get(0).and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                                lon_builder.append_option(coords.get(0).and_then(|v| v.as_f64()));
                                lat_builder.append_option(coords.get(1).and_then(|v| v.as_f64()));
                            } else {
                                lon_builder.append_null(); lat_builder.append_null();
                            }
                        } else {
                            lon_builder.append_null(); lat_builder.append_null();
                        }
                    }
                }
            }
        }
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("latitude", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(title_builder.finish()),
            Arc::new(cat_builder.finish()),
            Arc::new(lon_builder.finish()),
            Arc::new(lat_builder.finish()),
        ]).map_err(|e| format!("Failed to build EONET batch: {}", e))
    }
}


pub struct PolymarketParser;
impl SourceParser for PolymarketParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut title_builder = StringBuilder::new();
        let mut volume_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload) {
                if let Some(events) = val.as_array() {
                    for event in events {
                        title_builder.append_option(event.get("title").and_then(|v| v.as_str()));
                        volume_builder.append_option(event.get("volume").and_then(|v| v.as_f64()));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, true),
            Field::new("volume", DataType::Float64, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(title_builder.finish()),
            Arc::new(volume_builder.finish()),
        ]).map_err(|e| format!("Failed to build Polymarket batch: {}", e))
    }
}


pub struct CloudflareRadarParser;
impl SourceParser for CloudflareRadarParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut asn_builder = Int64Builder::new();
        let mut type_builder = StringBuilder::new();
        let mut country_builder = StringBuilder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                // Cloudflare nests events under result -> events
                if let Some(events) = val.get("result").and_then(|r| r.get("events")).and_then(|e| e.as_array()) {
                    for event in events {
                        asn_builder.append_option(event.get("asn").and_then(|v| v.as_i64()));
                        type_builder.append_option(event.get("leak_type").and_then(|v| v.as_str()));
                        country_builder.append_option(event.get("country_code").and_then(|v| v.as_str()));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("asn", DataType::Int64, true),
            Field::new("leak_type", DataType::Utf8, true),
            Field::new("country_code", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(asn_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(country_builder.finish()),
        ]).map_err(|e| format!("Failed to build Cloudflare Radar batch: {}", e))
    }
}


pub struct NasaFirmsParser;
impl SourceParser for NasaFirmsParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut lat_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut bright_builder = Float64Builder::new();
        let mut conf_builder = StringBuilder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                // Assuming a JSON-wrapped FIRMS response for simplicity
                if let Some(array) = val.as_array() {
                    for item in array {
                        lat_builder.append_option(item.get("latitude").and_then(|v| v.as_f64()));
                        lon_builder.append_option(item.get("longitude").and_then(|v| v.as_f64()));
                        bright_builder.append_option(item.get("bright_ti4").and_then(|v| v.as_f64()));
                        conf_builder.append_option(item.get("confidence").and_then(|v| v.as_str()));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("brightness", DataType::Float64, true),
            Field::new("confidence", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(lat_builder.finish()),
            Arc::new(lon_builder.finish()),
            Arc::new(bright_builder.finish()),
            Arc::new(conf_builder.finish()),
        ]).map_err(|e| format!("Failed to build NASA FIRMS batch: {}", e))
    }
}


pub struct UrlhausParser;
impl SourceParser for UrlhausParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut id_builder = Int64Builder::new();
        let mut url_builder = StringBuilder::new();
        let mut status_builder = StringBuilder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(urls) = val.get("urls").and_then(|u| u.as_array()) {
                    for item in urls {
                        // IDs sometimes come in as strings depending on the abuse.ch endpoint
                        let id = item.get("id").and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
                        id_builder.append_option(id);
                        url_builder.append_option(item.get("url").and_then(|v| v.as_str()));
                        status_builder.append_option(item.get("url_status").and_then(|v| v.as_str()));
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(id_builder.finish()),
            Arc::new(url_builder.finish()),
            Arc::new(status_builder.finish()),
        ]).map_err(|e| format!("Failed to build URLhaus batch: {}", e))
    }
}


pub struct FredParser;
impl SourceParser for FredParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut date_builder = StringBuilder::new();
        let mut value_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<Value>(payload) {
                if let Some(obs) = json.get("observations").and_then(|o| o.as_array()) {
                    for item in obs {
                        date_builder.append_option(item.get("date").and_then(|v| v.as_str()));
                        // FRED sometimes returns "." for missing data, so we safely parse it
                        let val = item.get("value").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());
                        value_builder.append_option(val);
                    }
                }
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(date_builder.finish()), Arc::new(value_builder.finish())])
            .map_err(|e| format!("Failed to build FRED batch: {}", e))
    }
}

pub struct UcdpParser;
impl SourceParser for UcdpParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut id_builder = Int64Builder::new();
        let mut conflict_builder = StringBuilder::new();
        let mut lat_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut deaths_builder = Int32Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<Value>(payload) {
                if let Some(results) = json.get("Result").and_then(|r| r.as_array()) {
                    for item in results {
                        id_builder.append_option(item.get("id").and_then(|v| v.as_i64()));
                        conflict_builder.append_option(item.get("conflict_name").and_then(|v| v.as_str()));
                        lat_builder.append_option(item.get("latitude").and_then(|v| v.as_f64()));
                        lon_builder.append_option(item.get("longitude").and_then(|v| v.as_f64()));
                        deaths_builder.append_option(item.get("best").and_then(|v| v.as_i64().map(|d| d as i32)));
                    }
                }
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("conflict_name", DataType::Utf8, true),
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("deaths", DataType::Int32, true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(id_builder.finish()), Arc::new(conflict_builder.finish()),
            Arc::new(lat_builder.finish()), Arc::new(lon_builder.finish()), Arc::new(deaths_builder.finish()),
        ]).map_err(|e| format!("Failed to build UCDP batch: {}", e))
    }
}

pub struct OrefParser;
impl SourceParser for OrefParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut id_builder = StringBuilder::new();
        let mut title_builder = StringBuilder::new();
        let mut cities_builder = StringBuilder::new(); // Flattened array of cities

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<Value>(payload) {
                // OREF root is the object itself
                id_builder.append_option(json.get("id").and_then(|v| v.as_str()));
                title_builder.append_option(json.get("title").and_then(|v| v.as_str()));
                
                if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                    let cities: Vec<&str> = data.iter().filter_map(|v| v.as_str()).collect();
                    cities_builder.append_value(cities.join(", "));
                } else {
                    cities_builder.append_null();
                }
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
            Field::new("cities", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(id_builder.finish()), Arc::new(title_builder.finish()), Arc::new(cities_builder.finish())
        ]).map_err(|e| format!("Failed to build OREF batch: {}", e))
    }
}

pub struct CoinGeckoParser;
impl SourceParser for CoinGeckoParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut coin_builder = StringBuilder::new();
        let mut price_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<Value>(payload) {
                if let Some(obj) = json.as_object() {
                    for (coin, data) in obj {
                        coin_builder.append_value(coin);
                        price_builder.append_option(data.get("usd").and_then(|v| v.as_f64()));
                    }
                }
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("coin", DataType::Utf8, true),
            Field::new("price_usd", DataType::Float64, true),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(coin_builder.finish()), Arc::new(price_builder.finish())])
            .map_err(|e| format!("Failed to build CoinGecko batch: {}", e))
    }
}

pub struct OpenMeteoParser;
impl SourceParser for OpenMeteoParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut lat_builder = Float64Builder::new();
        let mut lon_builder = Float64Builder::new();
        let mut temp_builder = Float64Builder::new();

        for payload in payloads {
            if let Ok(json) = serde_json::from_str::<Value>(payload) {
                lat_builder.append_option(json.get("latitude").and_then(|v| v.as_f64()));
                lon_builder.append_option(json.get("longitude").and_then(|v| v.as_f64()));
                temp_builder.append_option(
                    json.get("current_weather").and_then(|c| c.get("temperature")).and_then(|v| v.as_f64())
                );
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("temperature", DataType::Float64, true),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(lat_builder.finish()), Arc::new(lon_builder.finish()), Arc::new(temp_builder.finish())])
            .map_err(|e| format!("Failed to build OpenMeteo batch: {}", e))
    }
}

// -- Telegram

pub struct TelegramParser;
impl SourceParser for TelegramParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut id_builder = Int64Builder::new();
        let mut channel_builder = StringBuilder::new();
        let mut text_builder = StringBuilder::new();
        let mut date_builder = Int64Builder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload) {
                id_builder.append_option(val.get("message_id").and_then(|v| v.as_i64()));
                channel_builder.append_option(val.get("channel").and_then(|v| v.as_str()));
                text_builder.append_option(val.get("text").and_then(|v| v.as_str()));
                date_builder.append_option(val.get("date").and_then(|v| v.as_i64()));
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("message_id", DataType::Int64, true),
            Field::new("channel", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("date", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(channel_builder.finish()),
                Arc::new(text_builder.finish()),
                Arc::new(date_builder.finish()),
            ],
        ).map_err(|e| format!("Failed to build Telegram batch: {}", e))
    }
}


// -- RSS

pub struct RssParser;
impl SourceParser for RssParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut title_builder = StringBuilder::new();
        let mut link_builder = StringBuilder::new();
        let mut pubdate_builder = StringBuilder::new();
        let mut desc_builder = StringBuilder::new();
        
        let mut meta_builder = StringBuilder::new(); 

        for payload in payloads {
            let mut reader = Reader::from_str(payload);
            reader.config_mut().trim_text(true);

            let mut buf = Vec::new();
            let mut in_item = false;
            let mut current_tag = String::new();
            
            let mut temp_title = String::new();
            let mut temp_link = String::new();
            let mut temp_pubdate = String::new();
            let mut temp_desc = String::new();
            
            // A temporary JSON map to hold all the unknown tags
            let mut temp_meta = serde_json::Map::new();

            loop {
                match reader.read_event_into(&mut buf) {
                    Ok(Event::Start(ref e)) => {
                        let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                        if tag_name == "item" {
                            in_item = true;
                            temp_title.clear();
                            temp_link.clear();
                            temp_pubdate.clear();
                            temp_desc.clear();
                            temp_meta.clear(); 
                        }
                        current_tag = tag_name;
                    }
                    Ok(Event::Text(e)) => {
                        if in_item {
                            if let Ok(text) = e.unescape() {
                                let text_str = text.to_string();
                                match current_tag.as_str() {
                                    "title" => temp_title.push_str(&text_str),
                                    "link" => temp_link.push_str(&text_str),
                                    "pubdate" => temp_pubdate.push_str(&text_str),
                                    "description" => temp_desc.push_str(&text_str),
                                    _ if !current_tag.is_empty() => {
                                        // Save any unknown tag into the JSON map
                                        temp_meta.insert(current_tag.clone(), serde_json::Value::String(text_str));
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Ok(Event::CData(e)) => {
                        if in_item {
                            let text_str = String::from_utf8_lossy(e.as_ref()).to_string();
                            match current_tag.as_str() {
                                "title" => temp_title.push_str(&text_str),
                                "link" => temp_link.push_str(&text_str),
                                "pubdate" => temp_pubdate.push_str(&text_str),
                                "description" => temp_desc.push_str(&text_str),
                                _ if !current_tag.is_empty() => {
                                    // Catch-all for CDATA blocks
                                    temp_meta.insert(current_tag.clone(), serde_json::Value::String(text_str));
                                }
                                _ => {}
                            }
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                        if tag_name == "item" {
                            in_item = false;
                            title_builder.append_value(temp_title.trim());
                            link_builder.append_value(temp_link.trim());
                            pubdate_builder.append_value(temp_pubdate.trim());
                            desc_builder.append_value(temp_desc.trim());
                            
                            let meta_json = serde_json::to_string(&temp_meta).unwrap_or_else(|_| "{}".to_string());
                            meta_builder.append_value(meta_json);
                        }
                        current_tag.clear();
                    }
                    Ok(Event::Eof) => break,
                    Err(e) => {
                        tracing::error!("XML parsing error: {:?}", e);
                        break;
                    }
                    _ => (),
                }
                buf.clear();
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, true),
            Field::new("link", DataType::Utf8, true),
            Field::new("pubDate", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true), 
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(title_builder.finish()),
            Arc::new(link_builder.finish()),
            Arc::new(pubdate_builder.finish()),
            Arc::new(desc_builder.finish()),
            Arc::new(meta_builder.finish()), 
        ]).map_err(|e| format!("Failed to build RSS batch: {}", e))
    }
}