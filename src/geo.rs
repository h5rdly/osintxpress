use std::sync::Arc;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

use arrow::array::{RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

use geoarrow::array::{MultiLineStringBuilder, GeoArrowArray, IntoArrow};
use geoarrow::datatypes::{CoordType, Dimension, MultiLineStringType};

use geo_types::Geometry;

use crate::client::{HttpClient, WreqClient};


#[pyfunction]
pub fn fetch_submarine_cables() -> PyResult<PyTable> {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let client = WreqClient::new();
        let mut headers = std::collections::HashMap::new();
        headers.insert("User-Agent".to_string(), "OsintXpress/0.3.0".to_string());

        // 1. Fetch bytes via Rust (No Python GIL!)
        let url = "https://raw.githubusercontent.com/telegeography/www.submarinecablemap.com/master/web/public/api/v3/cable/cable-geo.json";
        let bytes = client.get(url, Some(headers)).await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to fetch cables: {}", e)))?;

        let geojson_str = String::from_utf8(bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid UTF-8: {}", e)))?;

        // 2. Parse into GeoRust structs
        let feature_collection: geojson::FeatureCollection = geojson_str.parse()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("GeoJSON parse error: {}", e)))?;

        // 3. Initialize Standard Arrow Builders for metadata
        let mut name_builder = StringBuilder::new();
        let mut id_builder = StringBuilder::new();

        // 4. Initialize the GeoArrow Builder for the geometry
        let ml_type = MultiLineStringType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated);
        let mut geom_builder = MultiLineStringBuilder::new(ml_type);

        // 5. The Core Loop: Map GeoJSON to Arrow Memory
        for feature in feature_collection.features {
            
            let name = feature.property("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
            let id = feature.property("id").and_then(|v| v.as_str()).unwrap_or("Unknown");

            name_builder.append_value(name);
            id_builder.append_value(id);

            // Extract and coerce the geometry to a MultiLineString
            if let Some(geom) = feature.geometry {
                // The geojson crate's `geo-types` feature allows this conversion!
                let geo_geom: Result<Geometry<f64>, _> = geom.try_into();
                if let Ok(g) = geo_geom {
                    match g {
                        Geometry::LineString(ls) => {
                            let mls = geo_types::MultiLineString(vec![ls]);
                            geom_builder.push_multi_line_string(Some(&mls)).unwrap();
                        },
                        Geometry::MultiLineString(mls) => {
                            geom_builder.push_multi_line_string(Some(&mls)).unwrap();
                        },
                        // Handle weird/empty geometries safely in 0.8.0 by passing None!
                        _ => geom_builder.push_multi_line_string(None::<&geo_types::MultiLineString<f64>>).unwrap(), 
                    }
                } else {
                    geom_builder.push_multi_line_string(None::<&geo_types::MultiLineString<f64>>).unwrap();
                }
            } else {
                geom_builder.push_multi_line_string(None::<&geo_types::MultiLineString<f64>>).unwrap();
            }
        }

        // 6. Finalize Builders into memory-safe Arrow Arrays
        let name_arr = name_builder.finish();
        let id_arr = id_builder.finish();
        let geom_arr = geom_builder.finish();

        let geom_ext_type = geom_arr.extension_type();
        let geom_field = geom_ext_type.to_field("geometry", true);

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
            geom_field,
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(name_arr),
            Arc::new(id_arr),
            geom_arr.into_array_ref(),
        ]).map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("RecordBatch error: {}", e)))?;

        // 7. Hand the zero-copy pointer to Python
        let py_table = PyTable::try_new(vec![batch], schema)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("PyTable error: {}", e)))?;

        Ok(py_table)
    })
}