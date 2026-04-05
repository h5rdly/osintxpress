![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml)

# Osintxpress

A fast OSINT data fetching library. Returns [Arrow](https://github.com/apache/arrow) arrays, so you can proceed with your favorite data / visualization tools.

## Installation
`pip install osintxpress`

- [arro3-core](https://github.com/kylebarron/arro3/tree/main/arro3-core) is the sole dependency, to accept Arrow arrays via the [PyCapsule](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) protocol. `PyArrow` works as well if needed - has more options (such as converting to CSV), but is much bulkier. 
- `polars` is used in the tests.
- Wheels for Win, Mac, Linux, Alpine. FreeBSD wheels can be gladly added once updated arrow / polars packages are available.

## Demo
Interactive flight map using [Panel](https://github.com/holoviz/panel) and [lonboard](https://github.com/developmentseed/lonboard). osintxpress related code from the [demo](https://github.com/h5rdly/osintxpress/blob/main/demo.py) -

```python
from osintxpress import OsintEngine, SourceAdapter


engine = OsintEngine(worker_threads=2)
engine.add_rest_source(name="flights", adapter=SourceAdapter.OPENSKY, poll_interval_sec=10)
engine.start_all()

.
.

def update_dashboard():

    data = engine.poll()
    if 'flights' in data:
        df = pl.from_arrow(data['flights']).drop_nulls(subset=['longitude', 'latitude'])
    .
    .
    
```

Run `panel serve demo.py`, then open http://localhost:5006/demo

 <img width="2281" height="1033" alt="floights" src="https://github.com/user-attachments/assets/36df86f2-30e7-4063-946c-d39fe296cf9b" />

## Dashboard

`osintxpress` comes with a complementary [dashboard](https://github.com/h5rdly/osintxpress/blob/main/osintxpress/dashboard.py). Used for testing, tweaking etc., it can provide a starting point for Python dashboard builders.

To start the dashboard - 

`panel serve /path/to/dashboard.py`

<img width="2437" height="1009" alt="dash" src="https://github.com/user-attachments/assets/1ce4bbac-4eaa-4774-9be7-24a9671c9f2e" />


## Supported Sources
osintxpress natively polls / streams from  - 

| Domain | Source | Description |
| :--- | :--- | :--- |
| **Geopolitics & Conflict** | **ACLED** | Armed Conflict Location & Event Data Project |
| | **UCDP** | Uppsala Conflict Data Program (Georeferenced events) |
| | **GDELT** | Global Database of Events, Language, and Tone |
| | **OREF** | Israel Home Front Command (Real-time rocket/drone alerts) |
| | **UNHCR** | Refugee and Internally Displaced Person (IDP) population flows |
| **Cyber & Infrastructure** | **Cloudflare Radar** | Global BGP leaks and route hijacks |
| | **URLhaus** | Active malware distribution URLs (abuse.ch) |
| | **Feodo Tracker** | Active Botnet Command & Control (C2) servers (abuse.ch) |
| | **Ransomware.live**| Recent ransomware group victim postings |
| | **NGA Warnings** | Maritime navigational warnings (e.g., Submarine cable repairs) |
| **Natural & Environmental**| **NASA FIRMS** | VIIRS Satellite thermal anomalies (Wildfires & kinetic impacts) |
| | **NASA EONET** | Earth Observatory Natural Event Tracker |
| | **USGS** | Global live earthquake feeds |
| | **NWS** | US National Weather Service active alerts |
| | **Open-Meteo** | Live global surface temperatures and weather |
| **Markets & Macro** | **Polymarket** | Decentralized prediction market odds and volume |
| | **FRED** | Federal Reserve Economic Data (Macro indices) |
| | **CoinGecko** | Real-time cryptocurrency prices |
| | **Binance** | Live crypto trade execution streams (WebSocket) |
| **Tracking & Telecom** | **OpenSky Network** | Live global ADS-B flight tracking |
| | **AIS Stream** | Live global maritime shipping positions (WebSocket) |
| | **CelesTrak** | Active satellite TLE data (Orbital surveillance) |
| | **Telegram** | Authenticated MTProto channel scraping via `grammers` |
| **News & Media** | **Google News** | Accelerated proxy for Reuters World News |
| | **BBC & Al Jazeera** | Breaking world news RSS feeds |


## Suggestions
Feel free to open a ticket or a discussion.
