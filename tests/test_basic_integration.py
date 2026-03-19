import sys, time, json, logging
import unittest
import polars as pl 

sys.path.append(__file__.replace('\\', '/').rsplit('/', 2)[0])
from osintxpress import OsintEngine, MockServer, RestAdapter, WsAdapter


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

class TestOsintEngineIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        print('\n--- Starting Rust Mock Server ---')
        cls.mock_server = MockServer(host='127.0.0.1', port=0)
        
        # ACLED Mock
        cls.mock_server.add_rest_route(
            method='GET',
            path='/api/acled/read',
            status_code=200,
            json_payload=json.dumps({'data': [{'event_id_cnty': 'ISR123', 'fatalities': '0'}]})
        )
        
        # OpenSky Mock
        cls.mock_server.add_rest_route(
            method='GET',
            path='/opensky/states/all',
            status_code=200,
            json_payload=json.dumps({
                "time": 1710760000, 
                "states": [
                    ["4b1814", "SWR123  ", "Switzerland", 1710760000, 1710760000, 8.54, 47.45]
                ]
            })
        )

        # 2. GDELT Mock
        cls.mock_server.add_rest_route(
            method='GET',
            path='/gdelt/v2/geo',
            status_code=200,
            json_payload=json.dumps({
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": "Protest", "url": "http://news.com/1"},
                        "geometry": {"type": "Point", "coordinates": [35.21, 31.76]}
                    }
                ]
            })
        )

        # Reuters Mock - Raw XML string
        cls.mock_server.add_rest_route(
            method='GET',
            path='/rss/reuters',
            status_code=200,
            raw_payload='<rss><channel><item><title>Test News</title><link>https://reuters.com/123</link></item></channel></rss>'
        )

        # AIS Stream WebSocket Mock
        cls.mock_server.add_ws_route(
            path='/ws/aisstream',
            messages=[json.dumps({
                'Message': {
                    'PositionReport': {
                        'UserID': 211123456, 
                        'Sog': 12.5
                    }
                }
            })]
        )
        
        cls.mock_server.start()


    @classmethod
    def tearDownClass(cls):

        print('\n--- Stopping Rust Mock Server ---')
        cls.mock_server.stop()
        del cls.mock_server


    def test_worldmonitor_backends(self):

        engine = OsintEngine(worker_threads=2)
        
        engine.add_rest_source(
            name='acled_conflict',
            url=f'{self.mock_server.http_url()}/api/acled/read',
            adapter=RestAdapter.ACLED, 
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name='gdelt_events',
            url=f'{self.mock_server.http_url()}/gdelt/v2/geo',
            adapter=RestAdapter.GDELT_GEOJSON,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name='opensky_flights',
            url=f'{self.mock_server.http_url()}/opensky/states/all',
            adapter=RestAdapter.OPENSKY,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name='reuters_news',
            url=f'{self.mock_server.http_url()}/rss/reuters',
            adapter=RestAdapter.RSS,
            poll_interval_sec=1
        )
        
        engine.add_ws_source(
            name='ais_maritime',
            url=f'{self.mock_server.ws_url()}/ws/aisstream',
            adapter=WsAdapter.AIS_STREAM
        )
        
        engine.start()
        time.sleep(1.5) 
        data = engine.poll()
        engine.stop()
        del engine
        
        assert 'acled_conflict' in data
        assert 'gdelt_events' in data
        assert 'opensky_flights' in data
        assert 'reuters_news' in data
        assert 'ais_maritime' in data
        
        assert self.mock_server.get_request_count('/api/acled/read') >= 1

        acled_df = pl.from_arrow(data['acled_conflict'])
        assert len(acled_df) >= 1
        assert acled_df['event_id_cnty'][0] == 'ISR123'

        ais_df = pl.from_arrow(data['ais_maritime'])
        assert len(ais_df) >= 1
        assert 'mmsi' in ais_df.columns
        assert ais_df['mmsi'][0] == 211123456
        assert ais_df['speed'][0] == 12.5

        opensky_df = pl.from_arrow(data["opensky_flights"])
        assert len(opensky_df) >= 1
        assert "icao24" in opensky_df.columns
        assert opensky_df["icao24"][0] == "4b1814"
        assert opensky_df["callsign"][0] == "SWR123" # Rust trims the whitespace
        assert opensky_df["latitude"][0] == 47.45

        gdelt_df = pl.from_arrow(data["gdelt_events"])
        assert len(gdelt_df) >= 1
        assert "longitude" in gdelt_df.columns
        assert gdelt_df["name"][0] == "Protest"
        assert gdelt_df["longitude"][0] == 35.21

        reuters_df = pl.from_arrow(data["reuters_news"])
        assert len(reuters_df) >= 1
        assert "title" in reuters_df.columns
        assert reuters_df["title"][0] == "Test News"
        assert reuters_df["link"][0] == "https://reuters.com/123"


if __name__ == '__main__':

    unittest.main(verbosity=2)