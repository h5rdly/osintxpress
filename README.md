![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml)

# Osintxpress

A fast OSINT data fetching library.

## Installation
`pip install osintxpress`

- `pyarrow` is the sole dependency, to accept [Arrow](https://github.com/apache/arrow) arrays on the Python side.
- `polars` is used in the tests.
- Wheels for Win, Mac, Linux, Alpine. FreeBSD wheels can be gladlt added once updated arrow / polars packages are available.

## Demo

```python
import json

import pyarrow as pa, polars as pl, geoarrow.pyarrow as ga, panel as pn

import lonboard
from lonboard.basemap import MaplibreBasemap, CartoStyle

from osintxpress import OsintEngine, SourceAdapter


engine = OsintEngine(worker_threads=2)
engine.add_source(
    name="flights",
    source_type='rest',
    adapter=SourceAdapter.OPENSKY,
    poll_interval_sec=10 
)
engine.start_all()


# Start empty Lonboard WebGL map
interactive_map = lonboard.Map(
    layers=[], 
    basemap=MaplibreBasemap(style=CartoStyle.DarkMatter), 
    view_state={"longitude": 34.7, "latitude": 31.5, "zoom": 5, "pitch": 45}
)

stats = pn.pane.Markdown("### Awaiting signal...", styles={"color": "#39FF14"})


def update_dashboard():

    data = engine.poll()
    if "flights" in data and len(data["flights"]) > 0:
    
        df = pl.from_arrow(data["flights"]).drop_nulls(subset=["longitude", "latitude"])
        pa_table = df.to_arrow()
        
        # Create a GeoArrow Extension Column 
        geometry_col = ga.make_point(pa_table["longitude"], pa_table["latitude"])
        pa_table = pa_table.append_column("geometry", geometry_col)
        
        # Stream the GeoArrow binary to the browser GPU
        new_layer = lonboard.ScatterplotLayer(
            table=pa_table,
            get_fill_color=[57, 255, 20, 200], # Fancy glowy Green
            get_radius=4000,
            radius_min_pixels=2
        )
        
        # Overwrite the map layers
        interactive_map.layers = [new_layer]
        stats.object = f"### Tracking **{len(df)}** aircraft"

# Fire the engine poll every 5 seconds
pn.state.add_periodic_callback(update_dashboard, period=5000)

# Render flights view
pn.Column(
    stats,
    pn.pane.IPyWidget(interactive_map, sizing_mode="stretch_both", min_height=700),
    styles={"background": "#121212", "padding": "20px"}
).servable()
```

Run `panel serve demo.py`, then open [localhost](http://localhost:5006/demo) - 

 <img width="2281" height="1033" alt="floights" src="https://github.com/user-attachments/assets/36df86f2-30e7-4063-946c-d39fe296cf9b" />
