import json, time, unittest
from osintxpress import OsintEngine, MockServer, RestAdapter, WsAdapter


class TestWorldMonitorIngestion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 1. Initialize the built-in Rust mock server on a random free port
        cls.mock = MockServer(host="127.0.0.1", port=0)
        
        # --- MOCK: ACLED (Conflict Events - REST JSON) ---
        # ACLED returns a nested "data" array
        cls.mock.add_rest_route(
            method="GET",
            path="/api/acled/read",
            status_code=200,
            json_payload=json.dumps({
                "count": 1,
                "data": [{
                    "event_id_cnty": "ISR123",
                    "event_date": "2026-03-18",
                    "event_type": "Explosions/Remote violence",
                    "latitude": "31.76",
                    "longitude": "35.21",
                    "fatalities": "0"
                }]
            })
        )

        # --- MOCK: GDELT (Global Events - REST GeoJSON) ---
        # GDELT v2 GEO API returns standard GeoJSON
        cls.mock.add_rest_route(
            method="GET",
            path="/gdelt/v2/geo",
            status_code=200,
            json_payload=json.dumps({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "properties": {"name": "Protest", "url": "http://news.com/1"},
                    "geometry": {"type": "Point", "coordinates": [35.21, 31.76]}
                }]
            })
        )

        # --- MOCK: OpenSky Network (Aviation - REST JSON Array-of-Arrays) ---
        # OpenSky returns a highly compressed array of arrays to save bandwidth
        cls.mock.add_rest_route(
            method="GET",
            path="/opensky/states/all",
            status_code=200,
            json_payload=json.dumps({
                "time": 1710760000,
                "states": [
                    # [icao24, callsign, origin_country, time_position, last_contact, lon, lat, ...]
                    ["4b1814", "SWR123  ", "Switzerland", 1710760000, 1710760000, 8.54, 47.45]
                ]
            })
        )

        # --- MOCK: Standard RSS (News/Advisories - REST XML) ---
        cls.mock.add_rest_route(
            method="GET",
            path="/rss/reuters",
            status_code=200,
            # Note: We can pass raw XML strings too, not just JSON!
            raw_payload="""<?xml version="1.0" encoding="UTF-8"?>
                <rss version="2.0">
                    <channel>
                        <item>
                            <title>Global Markets Rally</title>
                            <link>https://reuters.com/article/1</link>
                            <pubDate>Wed, 18 Mar 2026 12:00:00 GMT</pubDate>
                        </item>
                    </channel>
                </rss>"""
        )

        # --- MOCK: AISStream (Maritime - WebSocket JSON Firehose) ---
        # WebSockets receive a list of messages to push as soon as the client connects
        cls.mock.add_ws_route(
            path="/ws/aisstream",
            messages=[
                json.dumps({
                    "MessageType": "PositionReport",
                    "Message": {
                        "PositionReport": {
                            "UserID": 211123456, # MMSI
                            "Latitude": 35.0,
                            "Longitude": 15.0,
                            "Sog": 12.5 # Speed over ground
                        }
                    }
                })
            ]
        )
        
        # Start the background Rust server
        cls.mock.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock.stop()

    def test_worldmonitor_backends(self):
        # 1. Initialize the ingestion engine
        engine = OsintEngine(worker_threads=2)
        
        # 2. Wire up the sources to the dynamic mock URLs
        # mock.http_url() -> "http://127.0.0.1:49182"
        # mock.ws_url() -> "ws://127.0.0.1:49182"
        
        engine.add_rest_source(
            name="acled_conflict",
            url=f"{self.mock.http_url()}/api/acled/read",
            adapter=RestAdapter.ACLED, # Tells Rust to use the specific ACLED serde struct
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name="gdelt_events",
            url=f"{self.mock.http_url()}/gdelt/v2/geo",
            adapter=RestAdapter.GDELT_GEOJSON,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name="opensky_flights",
            url=f"{self.mock.http_url()}/opensky/states/all",
            adapter=RestAdapter.OPENSKY,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            name="reuters_news",
            url=f"{self.mock.http_url()}/rss/reuters",
            adapter=RestAdapter.RSS,
            poll_interval_sec=1
        )
        
        engine.add_ws_source(
            name="ais_maritime",
            url=f"{self.mock.ws_url()}/ws/aisstream",
            adapter=WsAdapter.AIS_STREAM
        )
        
        # 3. Execute
        engine.start()
        
        # Yield the thread briefly so Tokio can connect, fetch, parse, and buffer
        time.sleep(1.5) 
        
        # 4. Consume
        data = engine.poll()
        engine.stop()
        
        # 5. Assertions
        # Verify that all 5 radically different data structures were successfully
        # fetched, parsed by their respective Rust adapters, and converted into Apache Arrow
        self.assertIn("acled_conflict", data)
        self.assertIn("gdelt_events", data)
        self.assertIn("opensky_flights", data)
        self.assertIn("reuters_news", data)
        self.assertIn("ais_maritime", data)
        
        # Assuming `data` dict values are pyarrow Arrays or Polars DataFrames:
        self.assertEqual(len(data["acled_conflict"]), 1)
        self.assertEqual(len(data["gdelt_events"]), 1)
        self.assertEqual(len(data["opensky_flights"]), 1)
        self.assertEqual(len(data["reuters_news"]), 1)
        self.assertEqual(len(data["ais_maritime"]), 1)
        
        # Verify the Rust adapter correctly mapped the AIS nested JSON to a flat columnar schema
        ais_df = data["ais_maritime"]
        self.assertIn("mmsi", ais_df.columns)
        self.assertEqual(ais_df["mmsi"][0], 211123456)
        self.assertEqual(ais_df["speed"][0], 12.5)

        # Verify the Rust server was actually hit
        self.assertEqual(self.mock.get_request_count("/api/acled/read"), 1)


    