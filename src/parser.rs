use std::sync::Arc;

use arrow::array::{Float64Builder, Int32Builder, Int64Builder, StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use serde_json::Value;

use quick_xml::events::Event;
use quick_xml::Reader;

use crate::SourceAdapter;


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


pub struct RssParser;
impl SourceParser for RssParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut title_builder = StringBuilder::new();
        let mut link_builder = StringBuilder::new();

        for payload in payloads {
            let mut reader = Reader::from_str(payload);
            reader.config_mut().trim_text(true);

            let mut buf = Vec::new();
            let mut in_item = false;
            let mut current_tag = String::new();
            
            let mut temp_title = String::new();
            let mut temp_link = String::new();

            loop {
                match reader.read_event_into(&mut buf) {
                    Ok(Event::Start(ref e)) => {
                        let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                        if tag_name == "item" {
                            in_item = true;
                            temp_title.clear();
                            temp_link.clear();
                        }
                        current_tag = tag_name;
                    }
                    Ok(Event::Text(e)) => {
                        if in_item {
                            if let Ok(text) = e.unescape() {
                                if current_tag == "title" {
                                    temp_title.push_str(&text);
                                } else if current_tag == "link" {
                                    temp_link.push_str(&text);
                                }
                            }
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                        if tag_name == "item" {
                            in_item = false;
                            title_builder.append_value(&temp_title);
                            link_builder.append_value(&temp_link);
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
        ]));

        RecordBatch::try_new(schema, vec![
            Arc::new(title_builder.finish()),
            Arc::new(link_builder.finish()),
        ]).map_err(|e| format!("Failed to build RSS batch: {}", e))
    }
}


pub fn get_parser(adapter: &SourceAdapter) -> Box<dyn SourceParser> {

    match adapter {
        SourceAdapter::ACLED => Box::new(AcledParser),
        SourceAdapter::GDELT_GEOJSON => Box::new(GdeltParser),
        SourceAdapter::OPENSKY => Box::new(OpenSkyParser),
        SourceAdapter::REUTERS => Box::new(RssParser),
        SourceAdapter::AIS_STREAM => Box::new(AisStreamParser),
    }
}