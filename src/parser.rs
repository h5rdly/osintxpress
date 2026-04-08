use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::io::Cursor;

use arrow::array::{Float64Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::reader::{infer_json_schema, ReaderBuilder};

use quick_xml::events::Event;
use quick_xml::Reader;

use serde_json::Value;

use crate::ParserType;


//-- PARSER & ROUTING

pub trait SourceParser: Send + Sync {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String>;
}


pub fn get_parser(parser_type: ParserType) -> Box<dyn SourceParser> {
    match parser_type {
        ParserType::DynamicJson     => Box::new(DynamicJsonParser),

        ParserType::Binance         => Box::new(BinanceParser),
        ParserType::AisStream       => Box::new(AisStreamParser),
        ParserType::AisHub          => Box::new(AishubParser),
        ParserType::Marinesia       => Box::new(MarinesiaParser),
        ParserType::DigiTraffic     => Box::new(DigitrafficParser),
        ParserType::Acled           => Box::new(AcledParser),
        ParserType::OpenSky         => Box::new(OpenSkyParser),
        ParserType::GdeltGeojson    => Box::new(GdeltParser), 
        ParserType::NasaEonet       => Box::new(EonetParser),
        ParserType::Usgs            => Box::new(UsgsParser),
        ParserType::Urlhaus         => Box::new(UrlhausParser),
        ParserType::Fred            => Box::new(FredParser),
        ParserType::Oref            => Box::new(OrefParser),
        ParserType::CoinGecko       => Box::new(CoinGeckoParser),
        ParserType::OpenMeteo       => Box::new(OpenMeteoParser),
        ParserType::Polymarket      => Box::new(PolymarketParser),
        ParserType::CloudflareRadar => Box::new(CloudflareRadarParser),
        ParserType::NasaFirms       => Box::new(NasaFirmsParser),
        ParserType::Ucdp            => Box::new(UcdpParser),
        ParserType::FeodoTracker    => Box::new(FeodoTrackerParser),
        ParserType::RansomwareLive  => Box::new(RansomwareLiveParser),
        ParserType::NgaWarnings     => Box::new(NgaWarningsParser),
        ParserType::Unhcr           => Box::new(UnhcrParser),       
        ParserType::Celestrak       => Box::new(CelestrakParser),   

        ParserType::GoogleNewsReuters | ParserType::Nws | ParserType::Bbc | ParserType::AlJazeera 
            => Box::new(RssParser),

        ParserType::Telegram        => Box::new(TelegramParser),  
    }
}


// -- Dynamic runtime json parser for custom / rapidly changing sources

pub struct DynamicJsonParser;

impl SourceParser for DynamicJsonParser {
    fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
        let mut all_records = Vec::new();

        // Look for the array of data in the payload
        for payload in payloads {
            let _preview = if payload.len() > 500 { &payload[..500] } else { payload };
            println!("🪲 [DEBUG DynamicJson]: {}", _preview);
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                let array = if json.is_array() {
                    json.as_array().unwrap().clone()
                } else if let Some(obj) = json.as_object() {
                    let mut found = vec![];
                    for key in ["data", "features", "items", "result", "results", "observations"] {
                        if let Some(val) = obj.get(key) {
                            if let Some(arr) = val.as_array() {
                                found = arr.clone();
                                break;
                            }
                        }
                    }
                    found
                } else {
                    vec![]
                };
                all_records.extend(array);
            }
        }

        if all_records.is_empty() {
            return Err("DynamicParser: No data array found in JSON payload".to_string());
        }

        // Convert to Newline Delimited JSON (NDJSON)
        // Apache Arrow expects each row to be an Object on its own line.
        let mut ndjson_string = String::new();
        for record in all_records {
            if record.is_object() {
                let row_str = serde_json::to_string(&record).map_err(|e| e.to_string())?;
                ndjson_string.push_str(&row_str);
                ndjson_string.push('\n');
            }
        }

        if ndjson_string.is_empty() {
            return Err("DynamicParser: No valid JSON objects found to parse".to_string());
        }

        let mut cursor = Cursor::new(ndjson_string.as_bytes());

        let (schema, _) = infer_json_schema(&mut cursor, None).map_err(|e| e.to_string())?;
        cursor.set_position(0);

        let mut reader = ReaderBuilder::new(Arc::new(schema)).build(cursor)
            .map_err(|e| e.to_string())?;

        // Return the first batch
        if let Some(batch) = reader.next() {
            batch.map_err(|e| e.to_string())
        } else {
            Err("DynamicParser: Failed to build RecordBatch".to_string())
        }
    }
}


// -- IDE-friendly manual parser macro

macro_rules! define_manual_parser {
    (
        $struct_name:ident,
        fields: [ $( ($col_name:expr, $var_name:ident, $arr_type:ident, $builder_type:ident) ),* $(,)? ],
        extract: $extract_logic:expr
    ) => {
        pub struct $struct_name;
        impl SourceParser for $struct_name {
            fn parse(&self, payloads: &[String]) -> Result<RecordBatch, String> {
                $( let mut $var_name = $builder_type::new(); )*

                // Explicitly tell Rust the types of the closure arguments
                let extractor: fn(&String, $( &mut $builder_type ),*) = $extract_logic;
                
                for payload in payloads {
                    let _preview = if payload.len() > 500 { &payload[..500] } else { payload };
                    // println!("🪲 [DEBUG {}]: {}", stringify!($struct_name), _preview);
                    extractor(payload, $( &mut $var_name ),*);
                }

                // Build Schema and RecordBatch
                let schema = Arc::new(Schema::new(vec![
                    $( Field::new($col_name, DataType::$arr_type, true) ),*
                ]));

                RecordBatch::try_new(
                    schema, 
                    vec![ $( Arc::new($var_name.finish()) as Arc<dyn arrow::array::Array> ),* ]
                ).map_err(|e| format!("Failed to build batch for {}: {}", stringify!($struct_name), e))
            }
        }
    };
}


// -- Manual parser implementations


define_manual_parser!(
    PolymarketParser,
    fields: [
        ("title", title, Utf8, StringBuilder),
        ("volume", volume, Float64, Float64Builder)
    ],
    extract: |payload: &String, title, volume| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.as_array() {
                for event in events {
                    title.append_option(event.get("title").and_then(|v| v.as_str()));
                    volume.append_option(event.get("volume").and_then(|v| v.as_f64()));
                }
            }
        }
    }
);

define_manual_parser!(
    CloudflareRadarParser,
    fields: [
        ("asn", asn, Int64, Int64Builder),
        ("leak_type", leak_type, Utf8, StringBuilder),
        ("country_code", country_code, Utf8, StringBuilder)
    ],
    extract: |payload: &String, asn, leak_type, country_code| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.pointer("/result/events").and_then(|e| e.as_array()) {
                for event in events {
                    asn.append_option(event.get("asn").and_then(|v| v.as_i64()));
                    leak_type.append_option(event.get("leak_type").and_then(|v| v.as_str()));
                    country_code.append_option(event.get("country_code").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    NasaFirmsParser,
    fields: [
        ("latitude", lat, Float64, Float64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("brightness", bright, Float64, Float64Builder),
        ("confidence", conf, Utf8, StringBuilder)
    ],
    extract: |payload: &String, lat, lon, bright, conf| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.as_array() {
                for event in events {
                    lat.append_option(event.get("latitude").and_then(|v| v.as_f64()));
                    lon.append_option(event.get("longitude").and_then(|v| v.as_f64()));
                    // Alias handling is natively simple! Just look for "bright_ti4"
                    bright.append_option(event.get("bright_ti4").and_then(|v| v.as_f64()));
                    conf.append_option(event.get("confidence").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    UcdpParser,
    fields: [
        ("id", id, Int64, Int64Builder),
        ("conflict_name", conflict_name, Utf8, StringBuilder),
        ("latitude", lat, Float64, Float64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("deaths", deaths, Int32, Int32Builder)
    ],
    extract: |payload: &String, id, conflict_name, lat, lon, deaths| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.pointer("/Result").and_then(|e| e.as_array()) {
                for event in events {
                    id.append_option(event.get("id").and_then(|v| v.as_i64()));
                    conflict_name.append_option(event.get("conflict_name").and_then(|v| v.as_str()));
                    lat.append_option(event.get("latitude").and_then(|v| v.as_f64()));
                    lon.append_option(event.get("longitude").and_then(|v| v.as_f64()));
                    deaths.append_option(event.get("best").and_then(|v| v.as_i64()).map(|d| d as i32));
                }
            }
        }
    }
);

define_manual_parser!(
    FeodoTrackerParser,
    fields: [
        ("ip_address", ip, Utf8, StringBuilder),
        ("port", port, Int32, Int32Builder),
        ("malware", malware, Utf8, StringBuilder)
    ],
    extract: |payload: &String, ip, port, malware| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.as_array() {
                for event in events {
                    ip.append_option(event.get("ip_address").and_then(|v| v.as_str()));
                    port.append_option(event.get("port").and_then(|v| v.as_i64()).map(|p| p as i32));
                    malware.append_option(event.get("malware").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    RansomwareLiveParser,
    fields: [
        ("group_name", group_name, Utf8, StringBuilder),
        ("victim", victim, Utf8, StringBuilder),
        ("published", published, Utf8, StringBuilder)
    ],
    extract: |payload: &String, group_name, victim, published| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.as_array() {
                for event in events {
                    group_name.append_option(event.get("group_name").and_then(|v| v.as_str()));
                    victim.append_option(event.get("post_title").and_then(|v| v.as_str()));
                    published.append_option(event.get("published").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    NgaWarningsParser,
    fields: [
        ("navArea", nav_area, Utf8, StringBuilder),
        ("text", text, Utf8, StringBuilder)
    ],
    extract: |payload: &String, nav_area, text| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = json.pointer("/broadcast-warn").and_then(|e| e.as_array()) {
                for event in events {
                    nav_area.append_option(event.get("navArea").and_then(|v| v.as_str()));
                    text.append_option(event.get("text").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    BinanceParser,
    fields: [
        ("price", price, Float64, Float64Builder),
        ("quantity", quantity, Float64, Float64Builder)
    ],
    extract: |payload: &String, price, quantity| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            price.append_option(val.get("p").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()));
            quantity.append_option(val.get("q").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()));
        }
    }
);

define_manual_parser!(
    AcledParser,
    fields: [
        ("event_id_cnty", id, Utf8, StringBuilder),
        ("event_type", event_type, Utf8, StringBuilder),
        ("latitude", lat, Float64, Float64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("fatalities", fat, Int32, Int32Builder)
    ],
    extract: |payload: &String, id, event_type, lat, lon, fat| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                for item in data {
                    id.append_option(item.get("event_id_cnty").and_then(|v| v.as_str()));
                    event_type.append_option(item.get("event_type").and_then(|v| v.as_str()));
                    lat.append_option(item.get("latitude").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()));
                    lon.append_option(item.get("longitude").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()));
                    fat.append_option(item.get("fatalities").and_then(|v| v.as_str()).and_then(|s| s.parse::<i32>().ok()));
                }
            }
        }
    }
);

define_manual_parser!(
    OpenSkyParser,
    fields: [
        ("icao24", icao24, Utf8, StringBuilder),
        ("callsign", callsign, Utf8, StringBuilder),
        ("origin_country", origin_country, Utf8, StringBuilder),
        ("time_position", time_position, Int64, Int64Builder),
        ("last_contact", last_contact, Int64, Int64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("latitude", lat, Float64, Float64Builder)
    ],
    extract: |payload: &String, icao24, callsign, origin_country, time_position, last_contact, lon, lat| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            if let Some(states) = val.get("states").and_then(|s| s.as_array()) {
                for state in states {
                    if let Some(arr) = state.as_array() {
                        icao24.append_option(arr.get(0).and_then(|v| v.as_str()));
                        callsign.append_option(arr.get(1).and_then(|v| v.as_str()).map(|s| s.trim()));
                        origin_country.append_option(arr.get(2).and_then(|v| v.as_str()));
                        time_position.append_option(arr.get(3).and_then(|v| v.as_i64()));
                        last_contact.append_option(arr.get(4).and_then(|v| v.as_i64()));
                        lon.append_option(arr.get(5).and_then(|v| v.as_f64()));
                        lat.append_option(arr.get(6).and_then(|v| v.as_f64()));
                    }
                }
            }
        }
    }
);

define_manual_parser!(
    GdeltParser,
    fields: [
        ("name", name, Utf8, StringBuilder),
        ("url", url, Utf8, StringBuilder),
        ("longitude", lon, Float64, Float64Builder),
        ("latitude", lat, Float64, Float64Builder)
    ],
    extract: |payload: &String, name, url, lon, lat| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            if let Some(features) = val.get("features").and_then(|f| f.as_array()) {
                for feature in features {
                    if let Some(props) = feature.get("properties") {
                        name.append_option(props.get("name").and_then(|v| v.as_str()));
                        url.append_option(props.get("url").and_then(|v| v.as_str()));
                    } else {
                        name.append_null(); url.append_null();
                    }
                    if let Some(coords) = feature.get("geometry").and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                        lon.append_option(coords.get(0).and_then(|v| v.as_f64()));
                        lat.append_option(coords.get(1).and_then(|v| v.as_f64()));
                    } else {
                        lon.append_null(); lat.append_null();
                    }
                }
            }
        }
    }
);

define_manual_parser!(
    EonetParser,
    fields: [
        ("title", title, Utf8, StringBuilder),
        ("category", cat, Utf8, StringBuilder),
        ("longitude", lon, Float64, Float64Builder),
        ("latitude", lat, Float64, Float64Builder)
    ],
    extract: |payload: &String, title, cat, lon, lat| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            if let Some(events) = val.get("events").and_then(|e| e.as_array()) {
                for event in events {
                    title.append_option(event.get("title").and_then(|v| v.as_str()));
                    if let Some(cats) = event.get("categories").and_then(|c| c.as_array()) {
                        cat.append_option(cats.get(0).and_then(|c| c.get("title")).and_then(|v| v.as_str()));
                    } else { cat.append_null(); }
                    
                    if let Some(geoms) = event.get("geometry").and_then(|g| g.as_array()) {
                        if let Some(coords) = geoms.get(0).and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                            lon.append_option(coords.get(0).and_then(|v| v.as_f64()));
                            lat.append_option(coords.get(1).and_then(|v| v.as_f64()));
                        } else { lon.append_null(); lat.append_null(); }
                    } else { lon.append_null(); lat.append_null(); }
                }
            }
        }
    }
);

define_manual_parser!(
    UsgsParser,
    fields: [
        ("place", place, Utf8, StringBuilder),
        ("magnitude", mag, Float64, Float64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("latitude", lat, Float64, Float64Builder)
    ],
    extract: |payload: &String, place, mag, lon, lat| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            if let Some(features) = val.get("features").and_then(|f| f.as_array()) {
                for feature in features {
                    if let Some(props) = feature.get("properties") {
                        place.append_option(props.get("place").and_then(|v| v.as_str()));
                        mag.append_option(props.get("mag").and_then(|v| v.as_f64()));
                    } else {
                        place.append_null(); mag.append_null();
                    }
                    if let Some(coords) = feature.get("geometry").and_then(|g| g.get("coordinates")).and_then(|c| c.as_array()) {
                        lon.append_option(coords.get(0).and_then(|v| v.as_f64()));
                        lat.append_option(coords.get(1).and_then(|v| v.as_f64()));
                    } else {
                        lon.append_null(); lat.append_null();
                    }
                }
            }
        }
    }
);

define_manual_parser!(
    UrlhausParser,
    fields: [
        ("id", id, Int64, Int64Builder),
        ("url", url, Utf8, StringBuilder),
        ("status", status, Utf8, StringBuilder)
    ],
    extract: |payload: &String, id, url, status| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            if let Some(urls) = val.get("urls").and_then(|u| u.as_array()) {
                for item in urls {
                    let parsed_id = item.get("id").and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
                    id.append_option(parsed_id);
                    url.append_option(item.get("url").and_then(|v| v.as_str()));
                    status.append_option(item.get("url_status").and_then(|v| v.as_str()));
                }
            }
        }
    }
);

define_manual_parser!(
    FredParser,
    fields: [
        ("date", date, Utf8, StringBuilder),
        ("value", value, Float64, Float64Builder)
    ],
    extract: |payload: &String, date, value| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(obs) = json.get("observations").and_then(|o| o.as_array()) {
                for item in obs {
                    date.append_option(item.get("date").and_then(|v| v.as_str()));
                    let val = item.get("value").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok());
                    value.append_option(val);
                }
            }
        }
    }
);

define_manual_parser!(
    OrefParser,
    fields: [
        ("id", id, Utf8, StringBuilder),
        ("title", title, Utf8, StringBuilder),
        ("cities", cities, Utf8, StringBuilder)
    ],
    extract: |payload: &String, id, title, cities| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            id.append_option(json.get("id").and_then(|v| v.as_str()));
            title.append_option(json.get("title").and_then(|v| v.as_str()));
            
            if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                let c_list: Vec<&str> = data.iter().filter_map(|v| v.as_str()).collect();
                cities.append_value(c_list.join(", "));
            } else {
                cities.append_null();
            }
        }
    }
);

define_manual_parser!(
    CoinGeckoParser,
    fields: [
        ("coin", coin, Utf8, StringBuilder),
        ("price_usd", price, Float64, Float64Builder)
    ],
    extract: |payload: &String, coin, price| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(obj) = json.as_object() {
                for (k, data) in obj {
                    coin.append_value(k);
                    price.append_option(data.get("usd").and_then(|v| v.as_f64()));
                }
            }
        }
    }
);

define_manual_parser!(
    OpenMeteoParser,
    fields: [
        ("latitude", lat, Float64, Float64Builder),
        ("longitude", lon, Float64, Float64Builder),
        ("temperature", temp, Float64, Float64Builder)
    ],
    extract: |payload: &String, lat, lon, temp| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            lat.append_option(json.get("latitude").and_then(|v| v.as_f64()));
            lon.append_option(json.get("longitude").and_then(|v| v.as_f64()));
            temp.append_option(json.get("current_weather").and_then(|c| c.get("temperature")).and_then(|v| v.as_f64()));
        }
    }
);

define_manual_parser!(
    TelegramParser,
    fields: [
        ("message_id", id, Int64, Int64Builder),
        ("channel", channel, Utf8, StringBuilder),
        ("text", text, Utf8, StringBuilder),
        ("date", date, Int64, Int64Builder),
        ("media", media, Utf8, StringBuilder)
    ],
    extract: |payload: &String, id, channel, text, date, media| {
        if let Ok(val) = serde_json::from_str::<Value>(payload) {
            id.append_option(val.get("message_id").and_then(|v| v.as_i64()));
            channel.append_option(val.get("channel").and_then(|v| v.as_str()));
            text.append_option(val.get("text").and_then(|v| v.as_str()));
            date.append_option(val.get("date").and_then(|v| v.as_i64()));
            if let Some(m) = val.get("media").and_then(|v| v.as_str()) {
                media.append_value(m);
            } else {
                media.append_value("");
            }
        }
    }
);


define_manual_parser!(
    UnhcrParser,
    fields: [
        ("origin", origin, Utf8, StringBuilder),
        ("destination", destination, Utf8, StringBuilder),
        ("population", population, Int64, Int64Builder),
        ("date", date, Utf8, StringBuilder)
    ],
    extract: |payload: &String, origin, destination, population, date| {
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            // UNHCR usually nests data under "items" or "data"
            if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                for item in items {
                    origin.append_option(item.get("origin").and_then(|v| v.as_str()));
                    destination.append_option(item.get("destination").and_then(|v| v.as_str()));
                    population.append_option(item.get("population").and_then(|v| v.as_i64()));
                    date.append_option(item.get("date").and_then(|v| v.as_str()));
                }
            }
        }
    }
);


define_manual_parser!(
    CelestrakParser,
    fields: [
        ("object_name", object_name, Utf8, StringBuilder),
        ("object_id", object_id, Utf8, StringBuilder),
        ("tle_line1", tle_line1, Utf8, StringBuilder),
        ("tle_line2", tle_line2, Utf8, StringBuilder)
    ],
    extract: |payload: &String, object_name, object_id, tle_line1, tle_line2| {
        
        let lines: Vec<&str> = payload.lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect();
            
        for chunk in lines.chunks(3) {
            if chunk.len() == 3 {
                let name = chunk[0];
                let l1 = chunk[1];
                let l2 = chunk[2];

                // Validate that it's a TLE block (Line 1 starts with '1', Line 2 with '2')
                if l1.starts_with('1') && l2.starts_with('2') {
                    
                    // Extract the 5-digit NORAD ID from the string (chars 2-7) for the object_id
                    let id = if l1.len() >= 7 { l1[2..7].trim() } else { "Unknown" };
                    
                    object_name.append_value(name);
                    object_id.append_value(id);
                    tle_line1.append_value(l1);
                    tle_line2.append_value(l2);
                }
            }
        }
    }
);


define_manual_parser!(
    MarinesiaParser,
    fields: [
        ("mmsi", mmsi, Int64, Int64Builder),
        ("latitude", latitude, Float64, Float64Builder),
        ("longitude", longitude, Float64, Float64Builder),
        ("sog", sog, Float64, Float64Builder),
        ("cog", cog, Float64, Float64Builder),
        ("nav_status", nav_status, Int64, Int64Builder),
        ("name", name, Utf8, StringBuilder),
        ("ship_type", ship_type, Int64, Int64Builder),
        ("destination", destination, Utf8, StringBuilder)
    ],
    extract: |payload: &String, mmsi, latitude, longitude, sog, cog, nav_status, name, ship_type, destination| {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
            let now = chrono::Utc::now().timestamp();
            
            if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                for ship in data {
                    let mmsi_id = ship.get("mmsi").and_then(|v| v.as_i64()).unwrap_or(0);
                    let lat_val = ship.get("lat").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let lon_val = ship.get("lng").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let nav_val = ship.get("status").and_then(|v| v.as_i64()).unwrap_or(15);
                    
                    // Translate Marinesia Strings into Standard AIS Integers!
                    let type_str = ship.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    let mapped_type = match type_str {
                        "Cargo" => 70,
                        "Tanker" => 80,
                        "Passenger" => 60,
                        "Fishing" => 30,
                        "Pleasure Craft" => 37,
                        _ => 0,
                    };

                    let ship_name = ship.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let dest = ship.get("dest").and_then(|v| v.as_str()).unwrap_or("Unknown");

                    // Update Dark Ship Cache
                    if let Ok(mut cache) = crate::parser::AIS_CACHE.write() {
                        let entry = cache.entry(mmsi_id).or_insert_with(crate::parser::ShipMeta::default);
                        entry.last_seen = now;
                        entry.last_lat = lat_val;
                        entry.last_lon = lon_val;
                        entry.last_nav_status = nav_val;
                        entry.ship_type = mapped_type;
                        entry.name = ship_name.to_string();
                        entry.destination = dest.to_string();
                    }

                    mmsi.append_option(Some(mmsi_id));
                    latitude.append_option(Some(lat_val));
                    longitude.append_option(Some(lon_val));
                    sog.append_option(ship.get("sog").and_then(|v| v.as_f64()));
                    cog.append_option(ship.get("cog").and_then(|v| v.as_f64()));
                    nav_status.append_option(Some(nav_val));
                    name.append_option(Some(ship_name));
                    ship_type.append_option(Some(mapped_type));
                    destination.append_option(Some(dest));
                }
            }
        }
    }
);


define_manual_parser!(
    DigitrafficParser,
    fields: [
        ("mmsi", mmsi, Int64, Int64Builder),
        ("latitude", latitude, Float64, Float64Builder),
        ("longitude", longitude, Float64, Float64Builder),
        ("sog", sog, Float64, Float64Builder),
        ("cog", cog, Float64, Float64Builder),
        ("nav_status", nav_status, Int64, Int64Builder),
        ("name", name, Utf8, StringBuilder),
        ("ship_type", ship_type, Int64, Int64Builder),
        ("destination", destination, Utf8, StringBuilder)
    ],
    extract: |payload: &String, mmsi, latitude, longitude, sog, cog, nav_status, name, ship_type, destination| {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
            let now = chrono::Utc::now().timestamp();
            
            if let Some(features) = json.get("features").and_then(|v| v.as_array()) {
                for f in features {
                    let props = f.get("properties");
                    let geom = f.get("geometry");
                    let coords = geom.and_then(|g| g.get("coordinates")).and_then(|c| c.as_array());

                    if let (Some(p), Some(c)) = (props, coords) {
                        if c.len() >= 2 {
                            let mmsi_id = p.get("mmsi").and_then(|v| v.as_i64()).unwrap_or(0);
                            let lon_val = c[0].as_f64().unwrap_or(0.0);
                            let lat_val = c[1].as_f64().unwrap_or(0.0);
                            let nav_val = p.get("navStat").and_then(|v| v.as_i64()).unwrap_or(15);
                            
                            let mut current_name = "Baltic Vessel".to_string();
                            let mut current_type = 80;
                            let mut current_dest = "Unknown".to_string();

                            if let Ok(mut cache) = crate::parser::AIS_CACHE.write() {
                                let entry = cache.entry(mmsi_id).or_insert_with(crate::parser::ShipMeta::default);
                                
                                // Update live tracking state
                                entry.last_seen = now;
                                entry.last_lat = lat_val;
                                entry.last_lon = lon_val;
                                entry.last_nav_status = nav_val;

                                // If this is a new ship, set dummy metadata so the Python filter catches it.
                                if entry.name.is_empty() || entry.name == "Unknown" {
                                    entry.name = current_name.clone();
                                    entry.ship_type = current_type;
                                    entry.destination = current_dest.clone();
                                } else {
                                    current_name = entry.name.clone();
                                    current_type = entry.ship_type;
                                    current_dest = entry.destination.clone();
                                }
                            }

                            mmsi.append_option(Some(mmsi_id));
                            longitude.append_option(Some(lon_val));
                            latitude.append_option(Some(lat_val));
                            sog.append_option(p.get("sog").and_then(|v| v.as_f64()));
                            cog.append_option(p.get("cog").and_then(|v| v.as_f64()));
                            nav_status.append_option(Some(nav_val));
                            
                            name.append_option(Some(current_name.as_str()));
                            ship_type.append_option(Some(current_type));
                            destination.append_option(Some(current_dest.as_str()));
                        }
                    }
                }
            }
        }
    }
);


define_manual_parser!(
    AishubParser,
    fields: [
        ("mmsi", mmsi, Int64, Int64Builder),
        ("latitude", latitude, Float64, Float64Builder),
        ("longitude", longitude, Float64, Float64Builder),
        ("sog", sog, Float64, Float64Builder),
        ("cog", cog, Float64, Float64Builder),
        ("name", name, Utf8, StringBuilder),
        ("ship_type", ship_type, Int64, Int64Builder),
        ("destination", destination, Utf8, StringBuilder)
    ],
    extract: |payload: &String, mmsi, latitude, longitude, sog, cog, name, ship_type, destination| {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
            
            // AISHub returns an array: [ {Meta}, [ {Ship1}, {Ship2} ] ]
            if let Some(root_array) = json.as_array() {
                if root_array.len() == 2 {
                    // Extract the second element which contains the actual ships
                    if let Some(ships) = root_array[1].as_array() {
                        for ship in ships {
                            mmsi.append_option(ship.get("MMSI").and_then(|v| v.as_i64()));
                            latitude.append_option(ship.get("LATITUDE").and_then(|v| v.as_f64()));
                            longitude.append_option(ship.get("LONGITUDE").and_then(|v| v.as_f64()));
                            sog.append_option(ship.get("SOG").and_then(|v| v.as_f64()));
                            cog.append_option(ship.get("COG").and_then(|v| v.as_f64()));
                            name.append_option(ship.get("NAME").and_then(|v| v.as_str()));
                            ship_type.append_option(ship.get("TYPE").and_then(|v| v.as_i64()));
                            destination.append_option(ship.get("DEST").and_then(|v| v.as_str()));
                        }
                    }
                }
            }
        }
    }
);


#[derive(Clone, Default)]
pub struct ShipMeta {
    pub ship_type: i64,
    pub destination: String,
    pub name: String,
    pub last_seen: i64,
    pub last_lat: f64,
    pub last_lon: f64,
    pub last_nav_status: i64,
}

lazy_static::lazy_static! {
    pub static ref AIS_CACHE: RwLock<HashMap<i64, ShipMeta>> = RwLock::new(HashMap::new());
}

define_manual_parser!(
    AisStreamParser,
    fields: [
        ("mmsi", mmsi, Int64, Int64Builder),
        ("latitude", latitude, Float64, Float64Builder),
        ("longitude", longitude, Float64, Float64Builder),
        ("sog", sog, Float64, Float64Builder),
        ("cog", cog, Float64, Float64Builder),
        ("nav_status", nav_status, Int64, Int64Builder),
        ("name", name, Utf8, StringBuilder),
        ("ship_type", ship_type, Int64, Int64Builder),
        ("destination", destination, Utf8, StringBuilder)
    ],
    extract: |payload: &String, mmsi, latitude, longitude, sog, cog, nav_status, name, ship_type, destination| {
        
        if let Ok(json) = serde_json::from_str::<Value>(payload) {
            if let Some(msg_type) = json.get("MessageType").and_then(|v| v.as_str()) {
                // Static Data, save ShipType and Destination to the cache
                if msg_type == "ShipStaticData" {
                    if let Some(data) = json.get("Message").and_then(|m| m.get("ShipStaticData")) {
                        if let Some(user_id) = data.get("UserID").and_then(|v| v.as_i64()) {
                            let s_type = data.get("Type").and_then(|v| v.as_i64()).unwrap_or(0);
                            let dest = data.get("Destination").and_then(|v| v.as_str()).unwrap_or("Unknown").trim().to_string();
                            let ship_name = data.get("Name").and_then(|v| v.as_str()).unwrap_or("Unknown").trim().to_string();

                            if let Ok(mut cache) = AIS_CACHE.write() {
                                let entry = cache.entry(user_id).or_insert_with(ShipMeta::default);
                                entry.ship_type = s_type;
                                entry.destination = dest;
                                entry.name = ship_name;
                            }
                        }
                    }
                }

                // A position report. Update the cache and send to Apache Arrow
                else if msg_type == "PositionReport" {
                    let meta = json.get("MetaData");
                    let report = json.get("Message").and_then(|m| m.get("PositionReport"));

                    if let Some(user_id) = meta.and_then(|m| m.get("MMSI")).and_then(|v| v.as_i64()) {
                        
                        // current live data
                        let lat = meta.and_then(|m| m.get("latitude")).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let lon = meta.and_then(|m| m.get("longitude")).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let nav = report.and_then(|r| r.get("NavigationalStatus")).and_then(|v| v.as_i64()).unwrap_or(15);
                        let ship_name_opt = meta.and_then(|m| m.get("ShipName")).and_then(|v| v.as_str());
                        let now = chrono::Utc::now().timestamp();

                        let mut s_type = 0;
                        let mut dest = "Unknown".to_string();

                        // Open a write Lock to update state and read the static data safely
                        if let Ok(mut cache) = AIS_CACHE.write() {
                            let entry = cache.entry(user_id).or_insert_with(ShipMeta::default);
                            
                            // Update the state machine for Dark Ship detection
                            entry.last_seen = now;
                            entry.last_lat = lat;
                            entry.last_lon = lon;
                            entry.last_nav_status = nav;
                            
                            if let Some(name) = ship_name_opt {
                                if entry.name.is_empty() || entry.name == "Unknown" {
                                    entry.name = name.trim().to_string();
                                }
                            }

                            // Read the static data to pass to Apache Arrow
                            s_type = entry.ship_type;
                            dest = entry.destination.clone();
                        } 

                        mmsi.append_option(Some(user_id));
                        name.append_option(ship_name_opt);
                        latitude.append_option(Some(lat));
                        longitude.append_option(Some(lon));
                        
                        sog.append_option(report.and_then(|r| r.get("Sog")).and_then(|v| v.as_f64()));
                        cog.append_option(report.and_then(|r| r.get("Cog")).and_then(|v| v.as_f64()));
                        nav_status.append_option(Some(nav));
                        
                        ship_type.append_option(Some(s_type));
                        destination.append_option(Some(dest.as_str()));
                    }
                }
            }
        }
    }
);


define_manual_parser!(
    RssParser,
    fields: [
        ("title", title, Utf8, StringBuilder),
        ("link", link, Utf8, StringBuilder),
        ("pubDate", pubdate, Utf8, StringBuilder),
        ("description", desc, Utf8, StringBuilder),
        ("metadata", meta, Utf8, StringBuilder)
    ],
    extract: |payload: &String, title, link, pubdate, desc, meta| {
        let mut reader = Reader::from_str(payload);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut in_item = false;
        let mut current_tag = String::new();
        
        let mut temp_title = String::new();
        let mut temp_link = String::new();
        let mut temp_pubdate = String::new();
        let mut temp_desc = String::new();
        let mut temp_meta = serde_json::Map::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                    if tag_name == "item" {
                        in_item = true;
                        temp_title.clear(); temp_link.clear(); temp_pubdate.clear(); temp_desc.clear(); temp_meta.clear(); 
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
                                    temp_meta.insert(current_tag.clone(), Value::String(text_str));
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
                                temp_meta.insert(current_tag.clone(), Value::String(text_str));
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                    if tag_name == "item" {
                        in_item = false;
                        title.append_value(temp_title.trim());
                        link.append_value(temp_link.trim());
                        pubdate.append_value(temp_pubdate.trim());
                        desc.append_value(temp_desc.trim());
                        
                        let meta_json = serde_json::to_string(&temp_meta).unwrap_or_else(|_| "{}".to_string());
                        meta.append_value(meta_json);
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
);