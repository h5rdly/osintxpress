import json, sys, json, unittest

import polars as pl
import pyarrow as pa

sys.path.append(__file__.replace('\\', '/').rsplit('/', 2)[0])
from osintxpress import arrow_to_mlt, list_mlt_layers, mlt_to_geojson, mvt_to_geojson


class TestMltUtils(unittest.TestCase):

    def setUp(self):
        # 1. Create a Polars DataFrame with standard and edge-case data
        self.df = pl.DataFrame({
            'latitude': [32.0853, 31.7683, None],  # Tel Aviv, Jerusalem, and a missing location
            'longitude': [34.7818, 35.2137, None],
            'city': ['Tel Aviv', 'Jerusalem', 'Unknown'],
            'threat_level': [5, 3, 0]
        })
        
        # 2. Convert to a PyArrow RecordBatch 
        # (pyo3-arrow on the Rust side will seamlessly consume this via the C Data Interface)
        self.batch = self.df.to_arrow().to_batches()[0]


    def test_arrow_to_mlt_success(self):
        '''Test that valid Arrow data cleanly compresses to MLT bytes.'''
        mlt_bytes = arrow_to_mlt('israel_cities', self.batch)
        
        self.assertIsInstance(mlt_bytes, bytes)
        self.assertTrue(len(mlt_bytes) > 0, 'MLT byte payload should not be empty')


    def test_arrow_to_mlt_custom_columns(self):
        '''Test that the encoder handles custom coordinate column names.'''
        custom_df = pl.DataFrame({'lat': [10.0], 'lon': [20.0], 'name': ['Test']})
        custom_batch = custom_df.to_arrow().to_batches()[0]
        
        # Should fail with default 'latitude/longitude' expectations
        with self.assertRaises(ValueError):
            arrow_to_mlt('custom', custom_batch)
            
        # Should succeed when explicitly mapped
        mlt_bytes = arrow_to_mlt('custom', custom_batch, lat_col='lat', lon_col='lon')
        self.assertTrue(len(mlt_bytes) > 0)


    def test_list_mlt_layers(self):
        '''Test that we can peek into an MLT tile and read its layer names without full decoding.'''
        mlt_bytes = arrow_to_mlt('tactical_layer', self.batch)
        
        layers = list_mlt_layers(mlt_bytes)
        
        self.assertIsInstance(layers, list)
        self.assertEqual(len(layers), 1)
        self.assertEqual(layers[0], 'tactical_layer')


    def test_mlt_roundtrip_to_geojson(self):
        '''Test the full lifecycle: Arrow -> MLT -> GeoJSON and verify data integrity.'''
        mlt_bytes = arrow_to_mlt('israel_cities', self.batch)
        geojson_str = mlt_to_geojson(mlt_bytes)
        
        self.assertIsInstance(geojson_str, str)
        geo_dict = json.loads(geojson_str)
        
        self.assertEqual(geo_dict.get('type'), 'FeatureCollection')
        self.assertIn('features', geo_dict)
        
        # We passed 3 rows, but 1 had null coordinates. 
        # Our MltBridge explicitly skips null geometries, so we expect exactly 2 features.
        features = geo_dict['features']
        self.assertEqual(len(features), 2, 'Should have skipped the row with null coordinates')
        
        # Verify property integrity
        first_feature = features[0]
        self.assertEqual(first_feature['properties']['city'], 'Tel Aviv')
        self.assertEqual(first_feature['properties']['threat_level'], 5)
        # MLT internal layer tagging
        self.assertEqual(first_feature['properties']['_layer'], 'israel_cities') 


    def test_mlt_bad_data_handling(self):
        '''Ensure the decoders gracefully catch garbage bytes and raise Python ValueErrors.'''
        bad_bytes = b'this is definitely not a valid compressed maplibre tile'
        
        with self.assertRaises(ValueError) as context:
            list_mlt_layers(bad_bytes)
        self.assertIn('MLT Parse error', str(context.exception))
            
        with self.assertRaises(ValueError):
            mlt_to_geojson(bad_bytes)


    def test_mvt_bad_data_handling(self):
        '''Ensure the MVT decoder catches bad Protobuf data safely.'''
        bad_bytes = b'not a protobuf'
        
        with self.assertRaises(ValueError) as context:
            mvt_to_geojson(bad_bytes)
        self.assertIn('MVT Parse error', str(context.exception))


if __name__ == '__main__':
    unittest.main(verbosity=2)