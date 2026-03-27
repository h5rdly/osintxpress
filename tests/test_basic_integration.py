import sys, time, json, logging
import unittest
import polars as pl 

sys.path.append(__file__.replace('\\', '/').rsplit('/', 2)[0])

# Notice we now import the unified SourceAdapter instead of RestAdapter/WsAdapter
from osintxpress import OsintEngine, MockServer, SourceAdapter

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

class TestOsintEngineIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('\n--- Starting Rust Mock Server ---')
        cls.mock_server = MockServer(host='127.0.0.1', port=0)
        
        cls.mock_server.add_rest_route(
            path='/api/acled/read',
            json_payload=json.dumps({'data': [{'event_id_cnty': 'ISR123', 'fatalities': '0'}]})
        )
        
        cls.mock_server.add_rest_route( 
            path='/opensky/states/all',
            json_payload=json.dumps({
                'time': 1710760000, 
                'states': [
                    ['4b1814', 'SWR123  ', 'Switzerland', 1710760000, 1710760000, 8.54, 47.45]
                ]
            })
        )

        cls.mock_server.add_rest_route(
            path='/gdelt/v2/geo', 
            json_payload=json.dumps({
                'type': 'FeatureCollection',
                'features': [
                    {
                        'type': 'Feature',
                        'properties': {'name': 'Protest', 'url': 'http://news.com/1'},
                        'geometry': {'type': 'Point', 'coordinates': [35.21, 31.76]}
                    }
                ]
            })
        )

        # Reuters Mock - Raw XML string
        cls.mock_server.add_rest_route(
            path='/rss/reuters',
            raw_payload='<rss><channel><item><title>Test News</title><link>https://reuters.com/123</link></item></channel></rss>'
        )

        cls.mock_server.add_ws_route(
            path='/ws/aisstream',
            messages=[json.dumps({
                'MetaData': {
                    'MMSI': 211123456,
                    'ShipName': 'Test Ship',
                    'latitude': 32.0,
                    'longitude': 34.8
                },
                'Message': {
                    'PositionReport': {
                        'Sog': 12.5,
                        'TrueHeading': 90.0
                    }
                }
            })]
        )
        
        cls.mock_server.add_rest_route(
            '/news/world/rss.xml', 
            raw_payload='<rss><channel><item><title>BBC Test</title><link>bbc.com</link></item></channel></rss>'
        )
        
        cls.mock_server.add_rest_route(
            '/earthquakes/feed/v1.0/summary/all_hour.geojson', 
            json_payload=json.dumps({'features': [{'properties': {'place': '5km N of Test', 'mag': 4.5}, 'geometry': {'coordinates': [-120.0, 35.0]}}]})
        )
        
        cls.mock_server.add_ws_route(
            '/ws/btcusdt@trade', 
            messages=[json.dumps({'p': '65000.50', 'q': '0.150'})]
        )

        cls.mock_server.add_rest_route(
            '/api/v3/events', 
            json_payload=json.dumps({
                "events": [{
                    "title": "Wildfire Test", 
                    "categories": [{"title": "Wildfires"}], 
                    "geometry": [{"coordinates": [-120.5, 35.5]}]
                }]
            })
        )
        
        cls.mock_server.add_rest_route(
            '/events', 
            json_payload=json.dumps([
                {"title": "Will X happen?", "volume": 50000.0}
            ])
        )
        cls.mock_server.start()


    @classmethod
    def tearDownClass(cls):
        print('\n--- Stopping Rust Mock Server ---')
        cls.mock_server.stop()
        del cls.mock_server


    def test_worldmonitor_backends(self):

        engine = OsintEngine(worker_threads=2)
        base_url = self.mock_server.http_url

        engine.add_rest_source(
            url=f'{base_url}/api/acled/read',
            adapter=SourceAdapter.ACLED, 
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/gdelt/v2/geo',
            adapter=SourceAdapter.GDELT_GEOJSON,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/opensky/states/all',
            adapter=SourceAdapter.OPENSKY,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/rss/reuters',
            adapter=SourceAdapter.GOOGLE_NEWS_REUTERS,
            poll_interval_sec=1
        )
        
        engine.add_ws_source(
            url=f'{self.mock_server.ws_url}/ws/aisstream',
            adapter=SourceAdapter.AIS_STREAM
        )
        
        engine.add_rest_source(
            url=f'{base_url}/news/world/rss.xml',
            adapter=SourceAdapter.BBC,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/earthquakes/feed/v1.0/summary/all_hour.geojson',
            adapter=SourceAdapter.USGS,
            poll_interval_sec=1
        )
        
        engine.add_ws_source(
            url=f'{self.mock_server.ws_url}/ws/btcusdt@trade',
            adapter=SourceAdapter.BINANCE
        )

        engine.add_rest_source(
            url=f'{base_url}/api/v3/events',
            adapter=SourceAdapter.NASA_EONET,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/events',
            adapter=SourceAdapter.POLYMARKET,
            poll_interval_sec=1
        )

        engine.start_all()
        time.sleep(1.5) 
        data = engine.poll()
        engine.stop_all()
        del engine
        
        # Ensure all data streams returned payloads using the auto-generated names
        assert 'acled' in data
        assert 'ais_stream' in data
        assert 'opensky' in data
        assert 'gdelt_geojson' in data
        assert 'google_news_reuters' in data
        assert 'bbc' in data
        assert 'usgs' in data
        assert 'binance' in data
        assert 'nasa_eonet' in data
        assert 'polymarket' in data

        assert self.mock_server.get_request_count('/api/acled/read') >= 1

        # Check ACLED
        acled_df = pl.from_arrow(data['acled'])
        assert len(acled_df) >= 1
        assert acled_df['event_id_cnty'][0] == 'ISR123'

        # Check AIS
        ais_df = pl.from_arrow(data['ais_stream'])
        assert len(ais_df) >= 1
        assert 'mmsi' in ais_df.columns
        assert ais_df['mmsi'][0] == 211123456
        assert ais_df['speed'][0] == 12.5

        # Check OpenSky
        opensky_df = pl.from_arrow(data['opensky'])
        assert len(opensky_df) >= 1
        assert 'icao24' in opensky_df.columns
        assert opensky_df['icao24'][0] == '4b1814'
        assert opensky_df['callsign'][0] == 'SWR123' 
        assert opensky_df['latitude'][0] == 47.45

        # Check GDELT
        gdelt_df = pl.from_arrow(data['gdelt_geojson'])
        assert len(gdelt_df) >= 1
        assert 'longitude' in gdelt_df.columns
        assert gdelt_df['name'][0] == 'Protest'
        assert gdelt_df['longitude'][0] == 35.21

        # Check RSS (Reuters)
        reuters_df = pl.from_arrow(data['google_news_reuters'])
        assert len(reuters_df) >= 1
        assert 'title' in reuters_df.columns
        assert reuters_df['title'][0] == 'Test News'
        assert reuters_df['link'][0] == 'https://reuters.com/123'

        # Check BBC
        bbc_df = pl.from_arrow(data['bbc'])
        assert len(bbc_df) >= 1
        assert bbc_df['title'][0] == 'BBC Test'

        # Check USGS 
        usgs_df = pl.from_arrow(data['usgs'])
        assert len(usgs_df) >= 1
        assert usgs_df['magnitude'][0] == 4.5
        assert usgs_df['place'][0] == '5km N of Test'

        # Check Binance 
        crypto_df = pl.from_arrow(data['binance'])
        assert len(crypto_df) >= 1
        assert crypto_df['price'][0] == 65000.50
        assert crypto_df['quantity'][0] == 0.150

        # Check NASA EONET
        eonet_df = pl.from_arrow(data['nasa_eonet'])
        assert len(eonet_df) >= 1
        assert eonet_df['title'][0] == "Wildfire Test"
        assert eonet_df['category'][0] == "Wildfires"
        assert eonet_df['longitude'][0] == -120.5

        # Check Polymarket
        poly_df = pl.from_arrow(data['polymarket'])
        assert len(poly_df) >= 1
        assert poly_df['title'][0] == "Will X happen?"
        assert poly_df['volume'][0] == 50000.0


if __name__ == '__main__':

    unittest.main(verbosity=2)