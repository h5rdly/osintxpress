from dataclasses import dataclass, field

try:
    from .osintxpress import ParserType
except:
    from osintxpress import ParserType


@dataclass
class SourceConfig:
    parser_type: ParserType
    name: str
    default_url: str
    alt_urls: list[str] = field(default_factory=list)


class SourceAdapter:
    
    BINANCE = SourceConfig(
        ParserType.Binance, "BINANCE", "wss://stream.binance.com:9443/ws/btcusdt@trade", 
        ["wss://stream.binance.us:9443/ws/btcusdt@trade"]
    )
    AIS_STREAM = SourceConfig(
        ParserType.AisStream, "AIS_STREAM", "wss://stream.aisstream.io/v0/stream"
    )
    AISHUB = SourceConfig(
        ParserType.AisHub, "AISHUB", 
        "https://data.aishub.net/ws.php?username=YOUR_AISHUB_USERNAME&format=1&output=json&compress=0"
    )
    DIGITRAFFIC = SourceConfig(
        ParserType.DigiTraffic, "DIGITRAFFIC", 
        "https://meri.digitraffic.fi/api/ais/v1/locations"
    )
    MARINESIA = SourceConfig(
        ParserType.Marinesia, "MARINESIA", "https://api.marinesia.com/api/v2/vessel/area"
    )
    IMF_PORTWATCH = SourceConfig(
        ParserType.DynamicJson, "IMF_PORTWATCH", # Using the Dynamic parser for the generic ArcGIS endpoint
        "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query?where=1%3D1&outFields=*&orderByFields=Date+DESC&resultRecordCount=500&f=json"
    )
    ACLED = SourceConfig(
        ParserType.Acled, "ACLED", "https://acleddata.com/api/acled/read", 
        ["https://api.acleddata.com/acled/read"]
    )
    NASA_EONET = SourceConfig(
        ParserType.NasaEonet, "NASA_EONET", "https://eonet.gsfc.nasa.gov/api/v3/events"
    )
    CELESTRAK_MILITARY = SourceConfig(
        ParserType.Celestrak, "CELESTRAK_MILITARY", 
        "https://celestrak.org/NORAD/elements/gp.php?GROUP=military&FORMAT=tle"
    )
    CELESTRAK_RESOURCE = SourceConfig(
        ParserType.Celestrak, "CELESTRAK_RESOURCE", 
        "https://celestrak.org/NORAD/elements/gp.php?GROUP=resource&FORMAT=tle"
    )
    OPENSKY = SourceConfig(
        ParserType.OpenSky, "OPENSKY", "https://opensky-network.org/api/states/all"
    )
    GDELT_GEOJSON = SourceConfig(
        ParserType.GdeltGeojson, "GDELT_GEOJSON", "https://api.gdeltproject.org/api/v2/geo/geo?format=geojson"
    )
    GOOGLE_NEWS_REUTERS = SourceConfig(
        ParserType.GoogleNewsReuters, "GOOGLE_NEWS_REUTERS", "https://news.google.com/rss/search?q=site%3Areuters.com+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen"
    )
    POLYMARKET = SourceConfig(
        ParserType.Polymarket, "POLYMARKET", "https://gamma-api.polymarket.com/events?limit=50&active=true"
    )
    USGS = SourceConfig(
        ParserType.Usgs, "USGS", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    )
    NWS = SourceConfig(
        ParserType.Nws, "NWS", "https://api.weather.gov/alerts/active"
    )
    BBC = SourceConfig(
        ParserType.Bbc, "BBC", "http://feeds.bbci.co.uk/news/world/rss.xml"
    )
    AL_JAZEERA = SourceConfig(
        ParserType.AlJazeera, "AL_JAZEERA", "https://www.aljazeera.com/xml/rss/all.xml"
    )
    CLOUDFLARE_RADAR = SourceConfig(
        ParserType.CloudflareRadar, "CLOUDFLARE_RADAR", "https://api.cloudflare.com/client/v4/radar/bgp/leaks/events"
    )
    NASA_FIRMS = SourceConfig(
        ParserType.NasaFirms, "NASA_FIRMS", "https://firms.modaps.eosdis.nasa.gov/api/country/csv/YOUR_KEY/VIIRS_SNPP_NRT/USA/1"
    )
    URLHAUS = SourceConfig(
        ParserType.Urlhaus, "URLHAUS", "https://urlhaus-api.abuse.ch/v1/urls/recent/"
    )
    FRED = SourceConfig(
        ParserType.Fred, "FRED", "https://api.stlouisfed.org/fred/series/observations?series_id=BDI&file_type=json"
    )
    UCDP = SourceConfig(
        ParserType.Ucdp, "UCDP", "https://ucdpapi.pcr.uu.se/api/gedevents/23.1?pagesize=100"
    )
    OREF = SourceConfig(
        ParserType.Oref, "OREF", "https://www.oref.org.il/WarningMessages/alert/alerts.json"
    )
    COINGECKO = SourceConfig(
        ParserType.CoinGecko, "COINGECKO", "https://api.coingecko.com/api/v3/simple/price?ids=tether,bitcoin&vs_currencies=usd"
    )
    OPEN_METEO = SourceConfig(
        ParserType.OpenMeteo, "OPEN_METEO", "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current_weather=true"
    )
    FEODO_TRACKER = SourceConfig(
        ParserType.FeodoTracker, "FEODO_TRACKER", "https://feodotracker.abuse.ch/downloads/ipblocklist.json"
    )
    RANSOMWARE_LIVE = SourceConfig(
        ParserType.RansomwareLive, "RANSOMWARE_LIVE", "https://api.ransomware.live/v2/recentvictims"
    )
    NGA_WARNINGS = SourceConfig(
        ParserType.NgaWarnings, "NGA_WARNINGS", "https://msi.nga.mil/api/publications/broadcast-warn?output=json"
    )
    UNHCR = SourceConfig(
        ParserType.Unhcr, "UNHCR", "https://api.unhcr.org/population/v1/population/"
    )
