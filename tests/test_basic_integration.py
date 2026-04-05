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

        cls.mock_server.add_rest_route(
            path='/radar/bgp',
            json_payload=json.dumps({"result": {"events": [{"asn": 13335, "leak_type": "hijack", "country_code": "US"}]}})
        )

        cls.mock_server.add_rest_route(
            path='/firms/viirs',
            json_payload=json.dumps([{"latitude": 34.5, "longitude": -118.2, "bright_ti4": 330.5, "confidence": "high"}])
        )

        cls.mock_server.add_rest_route(
            path='/urlhaus/recent',
            json_payload=json.dumps({"urls": [{"id": 12345, "url": "http://bad.com/mal", "url_status": "online"}]})
        )

        cls.mock_server.add_rest_route(
            path='/fred/obs',
            json_payload=json.dumps({"observations": [{"date": "2023-01-01", "value": "1500.5"}]})
        )

        cls.mock_server.add_rest_route(
            path='/ucdp/events',
            json_payload=json.dumps({"Result": [{"id": 1, "conflict_name": "Test War", "latitude": 10.0, "longitude": 20.0, "best": 5}]})
        )

        cls.mock_server.add_rest_route(
            path='/oref/alerts',
            json_payload=json.dumps({"id": "123", "title": "Missile", "data": ["Tel Aviv", "Bat Yam"]})
        )

        cls.mock_server.add_rest_route(
            path='/coingecko/price',
            json_payload=json.dumps({"tether": {"usd": 1.00}, "bitcoin": {"usd": 65000.0}})
        )

        cls.mock_server.add_rest_route(
            path='/meteo/current',
            json_payload=json.dumps({"latitude": 52.52, "longitude": 13.41, "current_weather": {"temperature": 22.5}})
        )

        cls.mock_server.add_rest_route(
            path='/feodo/ipblocklist.json', 
            json_payload=json.dumps([{"ip_address": "192.168.1.1", "port": 8080, "malware": "QakBot"}])
        )

        cls.mock_server.add_rest_route(
            path='/ransomware/recentvictims', 
            json_payload=json.dumps([{"group_name": "LockBit", "post_title": "MegaCorp", "published": "2023-10-01"}])
        )
        cls.mock_server.add_rest_route(
            path='/nga/broadcast-warn', 
            json_payload=json.dumps({"broadcast-warn": [{"navArea": "IV", "text": "SUBMARINE CABLE REPAIR OPERATING AT 34N 120W"}]})
        )

        cls.mock_server.add_rest_route(
            path='/unhcr/population',
            json_payload=json.dumps({"items": [{"origin": "Syria", "destination": "Turkey", "population": 3500000, "date": "2023"}]})
        )

        cls.mock_server.add_rest_route(
            path='/celestrak/tle',
            json_payload=json.dumps([{
                "OBJECT_NAME": "ISS (ZARYA)", 
                "OBJECT_ID": "1998-067A", 
                "TLE_LINE1": "1 25544U 98067A   20194.88612269 -.00002218  00000-0 -31515-4 0  9992",
                "TLE_LINE2": "2 25544  51.6461 221.2784 0001413  89.1723 280.4612 15.49507896236008"
            }])
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

        engine.add_ws_source(
            url=f'{self.mock_server.ws_url}/ws/aisstream',
            adapter=SourceAdapter.AIS_STREAM
        )

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

        engine.add_rest_source(
            url=f'{base_url}/radar/bgp',
            adapter=SourceAdapter.CLOUDFLARE_RADAR,
            poll_interval_sec=1
        )
        
        engine.add_rest_source(
            url=f'{base_url}/firms/viirs',
            adapter=SourceAdapter.NASA_FIRMS,
            poll_interval_sec=1
        )

        engine.add_rest_source(
            url=f'{base_url}/urlhaus/recent',
            adapter=SourceAdapter.URLHAUS,
            poll_interval_sec=1
        )
        engine.add_rest_source(url=f'{base_url}/feodo/ipblocklist.json', adapter=SourceAdapter.FEODO_TRACKER, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/ransomware/recentvictims', adapter=SourceAdapter.RANSOMWARE_LIVE, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/nga/broadcast-warn', adapter=SourceAdapter.NGA_WARNINGS, poll_interval_sec=1)


        engine.add_rest_source(url=f'{base_url}/fred/obs', adapter=SourceAdapter.FRED, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/ucdp/events', adapter=SourceAdapter.UCDP, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/oref/alerts', adapter=SourceAdapter.OREF, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/coingecko/price', adapter=SourceAdapter.COINGECKO, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/meteo/current', adapter=SourceAdapter.OPEN_METEO, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/unhcr/population', adapter=SourceAdapter.UNHCR, poll_interval_sec=1)
        engine.add_rest_source(url=f'{base_url}/celestrak/tle', adapter=SourceAdapter.CELESTRAK, poll_interval_sec=1)

        engine.start_all()

        expected_keys =  ['acled', 'ais_stream', 'opensky', 'gdelt_geojson', 'google_news_reuters', 
        'bbc', 'usgs', 'binance', 'nasa_eonet', 'polymarket', 'cloudflare_radar', 'nasa_firms', 
        'urlhaus', 'fred', 'ucdp', 'oref', 'coingecko', 'open_meteo', 'feodo_tracker', 'ransomware_live', 
        'nga_warnings', 'unhcr', 'celestrak']

        data = {}
        for _ in range(20):  
            time.sleep(0.5)
            poll_results = engine.poll()
            data.update(poll_results)
            
            if all(k in data for k in expected_keys):
                break

        engine.stop_all()
        del engine
                
        for key in expected_keys:
            assert key in data, f'key: {key} not in data'                

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

        # Check Cloudflare
        cf_df = pl.from_arrow(data['cloudflare_radar'])
        assert len(cf_df) >= 1
        assert cf_df['asn'][0] == 13335
        assert cf_df['leak_type'][0] == 'hijack'

        # Check FIRMS
        firms_df = pl.from_arrow(data['nasa_firms'])
        assert len(firms_df) >= 1
        assert firms_df['brightness'][0] == 330.5
        assert firms_df['confidence'][0] == 'high'

        # Check URLhaus
        urlhaus_df = pl.from_arrow(data['urlhaus'])
        assert len(urlhaus_df) >= 1
        assert urlhaus_df['id'][0] == 12345
        assert urlhaus_df['status'][0] == 'online'

        # Check FRED
        fred_df = pl.from_arrow(data['fred'])
        assert len(fred_df) >= 1
        assert 'date' in fred_df.columns
        assert fred_df['date'][0] == "2023-01-01"
        assert fred_df['value'][0] == 1500.5

        # Check UCDP
        ucdp_df = pl.from_arrow(data['ucdp'])
        assert len(ucdp_df) >= 1
        assert 'conflict_name' in ucdp_df.columns
        assert ucdp_df['id'][0] == 1
        assert ucdp_df['conflict_name'][0] == "Test War"
        assert ucdp_df['latitude'][0] == 10.0
        assert ucdp_df['longitude'][0] == 20.0
        assert ucdp_df['deaths'][0] == 5

        # Check OREF
        oref_df = pl.from_arrow(data['oref'])
        assert len(oref_df) >= 1
        assert 'cities' in oref_df.columns
        assert oref_df['id'][0] == "123"
        assert oref_df['title'][0] == "Missile"
        assert oref_df['cities'][0] == "Tel Aviv, Bat Yam"

        # Check CoinGecko
        coingecko_df = pl.from_arrow(data['coingecko'])
        assert len(coingecko_df) >= 2 # We mocked two coins: tether and bitcoin
        assert 'price_usd' in coingecko_df.columns
        
        # Because JSON object key iteration order isn't guaranteed, we extract the list to check
        coins = coingecko_df['coin'].to_list()
        assert "bitcoin" in coins
        assert "tether" in coins
        
        # Filter to Bitcoin and check its specific float value
        btc_price = coingecko_df.filter(pl.col("coin") == "bitcoin")['price_usd'][0]
        assert btc_price == 65000.0

        # Check OpenMeteo
        meteo_df = pl.from_arrow(data['open_meteo'])
        assert len(meteo_df) >= 1
        assert meteo_df['temperature'][0] == 22.5
        if 'feodo_tracker' not in data:
            raise RuntimeError(f"🚨 FATAL: feodo_tracker missing from FFI! Keys received: {list(data.keys())}")

        assert meteo_df['latitude'][0] == 52.52
        assert meteo_df['longitude'][0] == 13.41
        assert meteo_df['temperature'][0] == 22.5

        # Check FEODO
        feodo_df = pl.from_arrow(data['feodo_tracker'])
        assert len(feodo_df) >= 1
        assert feodo_df['ip_address'][0] == '192.168.1.1'
        assert feodo_df['malware'][0] == 'QakBot'

        # Check Ransomware
        ransom_df = pl.from_arrow(data['ransomware_live'])
        assert len(ransom_df) >= 1
        assert ransom_df['group_name'][0] == 'LockBit'
        assert ransom_df['victim'][0] == 'MegaCorp'

        # Check NGA
        nga_df = pl.from_arrow(data['nga_warnings'])
        assert len(nga_df) >= 1
        assert nga_df['navArea'][0] == 'IV'
        assert 'SUBMARINE CABLE' in nga_df['text'][0]
        
        # Check UNHCR
        unhcr_df = pl.from_arrow(data['unhcr'])
        assert len(unhcr_df) >= 1
        assert unhcr_df['origin'][0] == 'Syria'
        assert unhcr_df['destination'][0] == 'Turkey'
        assert unhcr_df['population'][0] == 3500000
        assert unhcr_df['date'][0] == '2023'

        # Check CelesTrak
        celestrak_df = pl.from_arrow(data['celestrak'])
        assert len(celestrak_df) >= 1
        assert celestrak_df['object_name'][0] == 'ISS (ZARYA)'
        assert 'latitude' in celestrak_df.columns
        assert 'longitude' in celestrak_df.columns
        assert celestrak_df['altitude_km'][0] > 100.0 # Satellites should be >100km up!

if __name__ == '__main__':

    unittest.main(verbosity=2)