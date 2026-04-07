import sys, time, json
import unittest
import polars as pl 

from osintxpress import OsintEngine, MockServer, SourceAdapter

class TestOsintEngineResilience(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mock_server = MockServer(host='127.0.0.1', port=0)
        
        # 1. Missing fields, wrong types, malformed JSON for ACLED
        cls.mock_server.add_rest_route(
            path='/api/poison',
            json_payload=json.dumps({'data': [{'event_id_cnty': 'BAD1', 'fatalities': {}}]})
        )

        # 2. Generate 2000 valid WS messages instantly to test the 1000-message buffer cap
        spam_messages = []
        for i in range(2000):
            spam_messages.append(json.dumps({
                'MessageType': 'PositionReport',
                'MetaData': {
                    'MMSI': i, 
                    'ShipName': f'SpamShip_{i}',
                    'latitude': 34.0,
                    'longitude': -118.0
                },
                'Message': {
                    'PositionReport': {
                        'Sog': 10.0,
                        'Cog': 180.0,
                        'NavigationalStatus': 0
                    }
                }
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
        engine.add_ws_source(
            name='ais_spam',
            url=f'{self.mock_server.ws_url}/ws/spam',
            adapter=SourceAdapter.AIS_STREAM
        )
        engine.start_all()
        
        time.sleep(2.5) 
        data = engine.poll()
        engine.stop_all()
        ais_df = pl.from_arrow(data['ais_spam'])
        
        # The engine should have protected itself by capping the output to 1000 rows
        self.assertEqual(len(ais_df), 1000, "Buffer cap failed! Memory leak potential.")
        
        # Check the LAST message received to verify we processed the entire batch
        last_mmsi = ais_df['mmsi'].to_list()[-1]
        self.assertEqual(last_mmsi, 1999, f"Engine did not finish processing! Ended on {last_mmsi}")
        
        # Check the FIRST message. If the last is 1999 and length is 1000, the first MUST be 1000
        first_mmsi = ais_df['mmsi'].to_list()[0]
        self.assertEqual(first_mmsi, 1000, "Did not properly drop the oldest messages!")
        
        print(f"\nBuffer capped successfully! Kept {len(ais_df)} latest messages.")


    def test_poison_payload_graceful_degradation(self):

        engine = OsintEngine(worker_threads=2)
        
        engine.add_rest_source(
            name='poison_feed',
            url=f'{self.mock_server.http_url}/api/poison',
            adapter=SourceAdapter.ACLED, 
            poll_interval_sec=1
        )
        
        engine.start_all()
        time.sleep(1.5)
        
        data = engine.poll()
        engine.stop_all()
        
        df = pl.from_arrow(data['poison_feed'])
        
        # We polled twice (1.5s), so we should have at least 1 row.
        self.assertTrue(len(df) >= 1, 'Should have returned rows')
        
        # The key assertion: The engine caught the bad JSON object and safely turned it into a null!
        self.assertTrue(df['fatalities'].is_null().all(), 'Malformed fields should gracefully degrade to null')
        self.assertEqual(df['event_id_cnty'][0], 'BAD1', 'Valid fields should still parse correctly')


if __name__ == '__main__':
    
    unittest.main()