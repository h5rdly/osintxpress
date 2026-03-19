use std::sync::Arc;
use arrow::array::{Float64Builder, Int64Builder, StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;

use crate::{RestAdapter, WsAdapter};

pub fn parse_ws_data(adapter: &WsAdapter, payloads: &[String]) -> RecordBatch {
    match adapter {
        WsAdapter::AIS_STREAM => {
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

            RecordBatch::try_new(schema, vec![mmsi_array, speed_array]).expect("Failed to build RecordBatch")
        }
    }
}

pub fn parse_rest_data(adapter: &RestAdapter, payloads: &[String]) -> RecordBatch {
    match adapter {
        RestAdapter::ACLED => {
            // 1. Initialize an Arrow string builder for the event IDs
            let mut id_builder = StringBuilder::new();

            // 2. Parse the JSON strings
            for payload in payloads {
                if let Ok(val) = serde_json::from_str::<Value>(payload) {
                    // ACLED data lives inside a nested "data" array
                    if let Some(data_array) = val.get("data").and_then(|d| d.as_array()) {
                        for item in data_array {
                            // Extract the string and append it to our Arrow column
                            id_builder.append_option(item.get("event_id_cnty").and_then(|v| v.as_str()));
                        }
                    }
                }
            }

            // 3. Freeze the column and define the schema
            let id_array = Arc::new(id_builder.finish());
            let schema = Arc::new(Schema::new(vec![
                Field::new("event_id_cnty", DataType::Utf8, true),
            ]));

            RecordBatch::try_new(schema, vec![id_array]).expect("Failed to build ACLED batch")
        }
        // Phase 2.2: We will implement GDELT, OPENSKY, and RSS next!
        _ => RecordBatch::new_empty(Arc::new(Schema::empty())),
    }
}