![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml)

# Osintxpress

A fast OSINT data fetching library. Returns [Arrow](https://github.com/apache/arrow) arrays, so you can proceed with your favorite data / visualization tools.

## Installation
`pip install osintxpress`

- `pyarrow` is the sole dependency, to accept Arrow arrays on the Python side.
- `polars` is used in the tests.
- Wheels for Win, Mac, Linux, Alpine. FreeBSD wheels can be gladly added once updated arrow / polars packages are available.

## Demo
Interactive flight map using [Panel](https://github.com/holoviz/panel) and [lonboard](https://github.com/developmentseed/lonboard). Fetching related code from the [demo](https://github.com/h5rdly/osintxpress/blob/main/demo.py) -

```python
from osintxpress import OsintEngine, SourceAdapter


engine = OsintEngine(worker_threads=2)
engine.add_source(name="flights", source_type='rest', adapter=SourceAdapter.OPENSKY, poll_interval_sec=10 )
engine.start_all()

.
.

def update_dashboard():

    data = engine.poll()
    if 'flights' in data and len(data['flights']) > 0:
        df = pl.from_arrow(data['flights']).drop_nulls(subset=['longitude', 'latitude'])
    .
    .
    
```

Run `panel serve demo.py`, then open [localhost](http://localhost:5006/demo).

 <img width="2281" height="1033" alt="floights" src="https://github.com/user-attachments/assets/36df86f2-30e7-4063-946c-d39fe296cf9b" />

## Supported Sources
osintxpress can currently pull from - 
- ACLED (https://acleddata.com/api/acled/read)
- OpenSky (https://opensky-network.org/api/states/all)
- GDELT GeoJSON (https://api.gdeltproject.org/api/v2/geo/geo?format=geojson)
- Reutres (https://www.reutersagency.com/feed)
- aisstream.io (wss://stream.aisstream.io/v0/stream)

## Suggestions
Feel free to open a ticket or a discussion!
