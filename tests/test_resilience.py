import sys, time, json
import unittest
import polars as pl 

from osintxpress import OsintEngine, MockServer, SourceAdapter

class TestOsintEngineResilience(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mock_server = MockServer(host='127.0.0.1', port=0)
        
        #  Missing fields, wrong types, malformed JSON
        cls.mock_server.add_rest_route(
            method='GET',
            path='/api/poison',
            status_code=200,
            # 'fatalities' expects an int/string, we give it a nested object
            # 'latitude' is missing
            json_payload=json.dumps({'data': [{'event_id_cnty': 'BAD1', 'fatalities': {}}]})
        )

        # Generate 100 WS messages instantly to test the buffer cap
        spam_messages = []
        for i in range(100):
            spam_messages.append(json.dumps({
                'MetaData': {'MMSI': i, 'ShipName': f'Ship {i}'},
                'Message': {'PositionReport': {'Sog': 10.0}}
            }))
            
        cls.mock_server.add_ws_route(
            path='/ws/spam',
            messages=spam_messages
        )
        
        cls.mock_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()


    def test_buffer_cap_prevents_oom(self):

        engine = OsintEngine(worker_threads=2)
        
        engine.add_source(
            name='ais_spam',
            url=f'{self.mock_server.ws_url}/ws/spam',
            source_type='ws',
            adapter=SourceAdapter.AIS_STREAM
        )
        
        engine.start_all()
        # Give the WS a moment to pull all 100 messages into the Rust buffer
        time.sleep(1.0) 
        
        data = engine.poll()
        engine.stop_all()
        
        ais_df = pl.from_arrow(data['ais_spam'])
        
        self.assertEqual(len(ais_df), 50, "Buffer cap failed! Memory leak potential.")
        self.assertEqual(ais_df['mmsi'][0], 50, "Did not drop the oldest messages!")


    def test_buffer_cap_prevents_oom(self):
        engine = OsintEngine(worker_threads=2)
        
        engine.add_source(
            name='ais_spam',
            url=f'{self.mock_server.ws_url}/ws/spam',
            source_type='ws',
            adapter=SourceAdapter.AIS_STREAM
        )
        
        engine.start_all()
        time.sleep(1.0) 
        
        data = engine.poll()
        engine.stop_all()
        
        ais_df = pl.from_arrow(data['ais_spam'])
        
        # The engine should have protected itself by dropping excess messages
        self.assertEqual(len(ais_df), 50, "Buffer cap failed! Memory leak potential.")
        print(f"\nBuffer capped successfully! Kept {len(ais_df)} latest messages.")


    def test_poison_payload_graceful_degradation(self):

        engine = OsintEngine(worker_threads=2)
        
        engine.add_source(
            name='poison_feed',
            url=f'{self.mock_server.http_url}/api/poison',
            source_type='rest',
            adapter=SourceAdapter.ACLED, 
            poll_interval_sec=1
        )
        
        engine.start_all()
        time.sleep(1.5)
        
        data = engine.poll()
        engine.stop_all()
        
        df = pl.from_arrow(data['poison_feed'])
        
        # We polled twice (1.5s), so we should have 2 rows.
        self.assertTrue(len(df) >= 1, "Should have returned rows")
        
        # The key assertion: The engine caught the bad JSON object and safely turned it into a null!
        self.assertTrue(df['fatalities'].is_null().all(), "Malformed fields should gracefully degrade to null")
        self.assertEqual(df['event_id_cnty'][0], 'BAD1', "Valid fields should still parse correctly")


