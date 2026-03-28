use arrow::record_batch::RecordBatch;
use arrow::array::{AsArray, Float64Array};
use arrow::datatypes::DataType;

use mlt_core::convert::geojson; 
use mlt_core::frames::layer::LayerEncoder;
use mlt_core::frames::model::{FeatureTable, Column, ScalarType};


pub struct MltBridge;

impl MltBridge {
    // Convert Apache Arrow RecordBatch to an MLT (MapLibre Tile) binary payload
    pub fn encode_from_arrow(
        layer_name: &str, 
        batch: &RecordBatch, 
        lat_col: &str, 
        lon_col: &str
    ) -> Result<Vec<u8>, String> {
        
        // Extract the coordinate arrays
        let lat_array = batch.column_by_name(lat_col)
            .ok_or_else(|| "Latitude column not found".to_string())?
            .as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| "Latitude must be Float64".to_string())?;
            
        let lon_array = batch.column_by_name(lon_col)
            .ok_or_else(|| "Longitude column not found".to_string())?
            .as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| "Longitude must be Float64".to_string())?;

        // Build the MLT Feature Table schema
        let mut columns = vec![
            Column {
                name: "geometry".to_string(),
                nullable: false,
                column_type: mlt_core::frames::model::ColumnType::Geometry,
            }
        ];

        // Map Arrow Schema to MLT Schema for properties
        for field in batch.schema().fields() {
            if field.name() == lat_col || field.name() == lon_col { continue; }
            
            let mlt_type = match field.data_type() {
                DataType::Utf8 => ScalarType::String,
                DataType::Float64 => ScalarType::Double,
                DataType::Int64 => ScalarType::Int64,
                DataType::Int32 => ScalarType::Int32,
                _ => continue, // Skip unsupported types for now
            };

            columns.push(Column {
                name: field.name().clone(),
                nullable: field.is_nullable(),
                column_type: mlt_core::frames::model::ColumnType::Scalar(mlt_type),
            });
        }

        let table = FeatureTable {
            name: layer_name.to_string(),
            extent: 4096, // Standard Web Mercator tile extent
            columns,
        };

        let mut encoder = LayerEncoder::new(table);
        
        for i in 0..batch.num_rows() {
            if lat_array.is_null(i) || lon_array.is_null(i) { continue; }
            
            // In a production setup, we'd use the MLT topology builders here.
            // For now, this is where you map the X/Y into the space-filling curve.
            let _lon = lon_array.value(i);
            let _lat = lat_array.value(i);
            
            // Encode the properties into the columnar streams
            // encoder.push_scalar(col_idx, value);
        }

        // Finalize compression (FastPFor, FSST, etc.) and return bytes
        let mlt_bytes = encoder.finish().map_err(|e| e.to_string())?;
        Ok(mlt_bytes)
    }
}


