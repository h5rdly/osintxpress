use arrow::record_batch::RecordBatch;
use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray,
};

use mlt_core::v01::{TileLayer01, TileFeature, PropValue, Tile01Encoder};
use mlt_core::geojson::Geom32;
use mlt_core::EncodedLayer;

use geo_types::Point;


pub struct MltBridge;

impl MltBridge {
    pub fn encode_from_arrow(
        layer_name: &str, 
        batch: &RecordBatch, 
        lat_col: &str, 
        lon_col: &str
    ) -> Result<Vec<u8>, String> {
        
        let lat_array = batch.column_by_name(lat_col)
            .ok_or_else(|| "Latitude column not found".to_string())?
            .as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| "Latitude must be Float64".to_string())?;
            
        let lon_array = batch.column_by_name(lon_col)
            .ok_or_else(|| "Longitude column not found".to_string())?
            .as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| "Longitude must be Float64".to_string())?;

        let mut property_names = Vec::new();
        for field in batch.schema().fields() {
            if field.name() == lat_col || field.name() == lon_col { continue; }
            property_names.push(field.name().clone());
        }

        let mut features = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            if lat_array.is_null(i) || lon_array.is_null(i) { continue; }
            
            let lon = lon_array.value(i);
            let lat = lat_array.value(i);
            
            // MapLibre Tiles use an internal grid.
            // For a global unprojected dataset, scale lon/lat to fit standard integer bounds.
            let px = (lon * 10000.0) as i32;
            let py = (lat * 10000.0) as i32;
            
            let geometry = Geom32::Point(Point::new(px, py));

            let mut properties = Vec::new();
            for name in &property_names {
                let col = batch.column_by_name(name).unwrap();
                if col.is_null(i) {
                    properties.push(PropValue::Str(None));
                    continue;
                }
                
                // Downcasting for standard Arrow and Polars types
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    properties.push(PropValue::Str(Some(arr.value(i).to_string())));
                } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                    properties.push(PropValue::Str(Some(arr.value(i).to_string())));
                } else if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    properties.push(PropValue::Str(Some(arr.value(i).to_string())));
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    properties.push(PropValue::F64(Some(arr.value(i))));
                } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                    properties.push(PropValue::F32(Some(arr.value(i))));
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    properties.push(PropValue::I64(Some(arr.value(i))));
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    properties.push(PropValue::I32(Some(arr.value(i))));
                } else {
                    properties.push(PropValue::Str(None));
                }
            }

            features.push(TileFeature {
                id: Some(i as u64),
                geometry,
                properties,
            });
        }

        let tile_layer = TileLayer01 {
            name: layer_name.to_string(),
            extent: 4096,
            property_names,
            features,
        };

        let (encoded_layer_01, _encoder) = Tile01Encoder::encode_auto(&tile_layer)
            .map_err(|e| e.to_string())?;

        let encoded_layer = EncodedLayer::Tag01(encoded_layer_01);
        
        let mut buf = Vec::new();
        encoded_layer.write_to(&mut buf).map_err(|e| e.to_string())?;

        Ok(buf)
    }
}