import time, json

import polars as pl

from osintxpress import OsintEngine, ParserType, MockServer
from osintxpress.sources import SourceConfig

import unittest


MOCK_SPACEX_DATA = [
    {"name": "FalconSat", "date_utc": "2006-03-24T22:30:00.000Z", "success": False, "flight_number": 1},
    {"name": "DemoSat", "date_utc": "2007-03-21T01:10:00.000Z", "success": False, "flight_number": 2},
    {"name": "RatSat", "date_utc": "2008-09-28T23:15:00.000Z", "success": True, "flight_number": 4},
    {"name": "Crew Dragon Demo-2", "date_utc": "2020-05-30T19:22:00.000Z", "success": True, "flight_number": 94}
]

class TestDynamicJsonParser(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):

        print(f'--- Testing Dynamic Json parser ---')
        cls.mock_server = MockServer(host='127.0.0.1', port=0)
        
        cls.mock_server.add_rest_route(
            path='/v4/launches/past',
            json_payload=json.dumps(MOCK_SPACEX_DATA)
        )
        
        cls.mock_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()


    def test_dynamic_parser_with_native_mock_server(self):

        engine = OsintEngine(worker_threads=1)
        
        mock_config = SourceConfig(
            parser_type=ParserType.DynamicJson, 
            name="SPACEX_MOCK", 
            default_url=f"{self.mock_server.http_url}/v4/launches/past"
        )
        
        engine.add_rest_source(mock_config, poll_interval_sec=2)
        engine.start_all()
        for _ in range(10):
            data = engine.poll()

            if not 'spacex_mock' in data:
                time.sleep(1)
                continue

            df = pl.from_arrow(data['spacex_mock'])
            self.assertEqual(len(df), 4)
            self.assertEqual(df['name'][2], "RatSat")
            self.assertTrue(df['success'][3])
            self.assertEqual(df['flight_number'][0], 1)
    
        engine.stop_all()
        

if __name__ == '__main__':

    unittest.main()
