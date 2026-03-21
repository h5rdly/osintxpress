use std::sync::Arc;

use serde_json::Value;
use quick_xml::events::Event;
use quick_xml::Reader;
use arrow::array::{Float64Builder, Int64Builder, StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use crate::SourceAdapter;


pub trait SourceParser: Send + Sync {
    fn parse(&self, payloads: &[String]) -> RecordBatch;
}


pub struct AisStreamParser;
impl SourceParser for AisStreamParser {
    fn parse(&self, payloads: &[String]) -> RecordBatch {
        let mut mmsi_builder = Int64Builder::with_capacity(payloads.len());
        let mut speed_builder = Float64Builder::with_capacity(payloads.len());

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(report) = val.get("Message").and_then(|m| m.get("PositionReport")) {
                    mmsi_builder.append_value(report.get("UserID").and_then(|v| v.as_i64()).unwrap_or(0));
                    speed_builder.append_value(report.get("Sog").and_then(|v| v.as_f64()).unwrap_or(0.0));
                }
            }
        }

        let mmsi_array = Arc::new(mmsi_builder.finish());
        let speed_array = Arc::new(speed_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("mmsi", DataType::Int64, false),
            Field::new("speed", DataType::Float64, false),
        ]));

        RecordBatch::try_new(schema, vec![mmsi_array, speed_array]).expect("Failed to build AIS RecordBatch")
    }
}


pub struct AcledParser;
impl SourceParser for AcledParser {
    fn parse(&self, payloads: &[String]) -> RecordBatch {
        let mut id_builder = StringBuilder::new();

        for payload in payloads {
            if let Ok(val) = serde_json::from_str::<Value>(payload) {
                if let Some(data_array) = val.get("data").and_then(|d| d.as_array()) {
                    for item in data_array {
                        id_builder.append_option(item.get("event_id_cnty").and_then(|v| v.as_str()));
                    }
                }
            }
        }

        let id_array = Arc::new(id_builder.finish());
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id_cnty", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(schema, vec![id_array]).expect("Failed to build ACLED batch")
    }
}


pub struct OpenSkyParser;
impl SourceParser for OpenSkyParser {
    fn parse(&self, payloads: &[String]) -> RecordBatch {
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
        ]).expect("Failed to build OpenSky batch")
    }
}


pub struct GdeltParser;
impl SourceParser for GdeltParser {
    fn parse(&self, payloads: &[String]) -> RecordBatch {
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
        ]).expect("Failed to build GDELT batch")
    }
}


pub struct RssParser;
impl SourceParser for RssParser {
    fn parse(&self, payloads: &[String]) -> RecordBatch {
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
        ]).expect("Failed to build RSS batch")
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