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
        
        # GDELT Mock (Empty stub for now)
        cls.mock_server.add_rest_route(
            method='GET',
            path='/gdelt/v2/geo',
            status_code=200,
            json_payload=json.dumps({'features': []})
        )

        # OpenSky Mock (Empty stub for now)
        cls.mock_server.add_rest_route(
            method='GET',
            path='/opensky/states/all',
            status_code=200,
            json_payload=json.dumps({'states': []})
        )

        # Reuters Mock - Raw XML string
        cls.mock_server.add_rest_route(
            method='GET',
            path='/rss/reuters',
            status_code=200,
            raw_payload='<rss><channel><item><title>Test News</title></item></channel></rss>'
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
        
        # Safe timing assertion
        assert self.mock_server.get_request_count('/api/acled/read') >= 1

        acled_df = pl.from_arrow(data['acled_conflict'])
        ais_df = pl.from_arrow(data['ais_maritime'])
        
        assert len(acled_df) >= 1
        assert len(ais_df) >= 1

        assert 'mmsi' in ais_df.columns
        assert ais_df['mmsi'][0] == 211123456
        assert ais_df['speed'][0] == 12.5



if __name__ == '__main__':
    unittest.main(verbosity=2)