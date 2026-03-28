import warnings
warnings.filterwarnings("ignore", message="No CRS exists on data")

import panel as pn
import polars as pl
import arro3.core as ac
import lonboard
from lonboard.basemap import MaplibreBasemap, CartoStyle
from geoarrow.rust.core import points 

from osintxpress import OsintEngine, SourceAdapter


## -- Engine Setup
engine = OsintEngine(worker_threads=4)
engine.add_rest_source(SourceAdapter.OPENSKY, poll_interval_sec=5)
engine.add_ws_source(SourceAdapter.AIS_STREAM)
engine.add_rest_source(SourceAdapter.USGS, poll_interval_sec=15)
engine.add_rest_source(SourceAdapter.NASA_EONET, poll_interval_sec=30)
engine.add_rest_source(SourceAdapter.ACLED, poll_interval_sec=60)
engine.add_rest_source(SourceAdapter.GOOGLE_NEWS_REUTERS, poll_interval_sec=60)
engine.add_rest_source(SourceAdapter.POLYMARKET, poll_interval_sec=30)

engine.start_all()


## -- UI Components & Templates
pn.extension('tabulator', sizing_mode="stretch_width")

# Sidebar Toggles (no names, we will use custom HTML labels)
switch_flights = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_ships = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_quakes = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_fires = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_acled = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))

# Color-coded labels matching the lonboard ScatterplotLayer RGB values!
layer_controls = pn.Column(
    "### 🗺️ Active Layers",
    pn.Row(switch_flights, pn.pane.HTML("<b style='color: rgb(57, 255, 20); font-size: 1.1em;'>✈️ Flights (OpenSky)</b>"), align='center'),
    pn.Row(switch_ships, pn.pane.HTML("<b style='color: rgb(0, 150, 255); font-size: 1.1em;'>🚢 Maritime (AIS)</b>"), align='center'),
    pn.Row(switch_quakes, pn.pane.HTML("<b style='color: rgb(255, 165, 0); font-size: 1.1em;'>🌋 Earthquakes (USGS)</b>"), align='center'),
    pn.Row(switch_fires, pn.pane.HTML("<b style='color: rgb(255, 255, 0); font-size: 1.1em;'>🔥 Wildfires (NASA)</b>"), align='center'),
    pn.Row(switch_acled, pn.pane.HTML("<b style='color: rgb(255, 50, 50); font-size: 1.1em;'>⚔️ Conflicts (ACLED)</b>"), align='center'),
)

system_stats = pn.pane.Markdown("🟢 **Engine Status:** Online\n📡 **Intercepting...**")

# Main Map
interactive_map = lonboard.Map(
    layers=[], 
    basemap=MaplibreBasemap(style=CartoStyle.DarkMatter), 
    view_state={"longitude": 10.0, "latitude": 40.0, "zoom": 2, "pitch": 25}
)
map_pane = pn.pane.IPyWidget(interactive_map, sizing_mode="stretch_both", min_height=550)

# Data Grids
news_grid = pn.widgets.Tabulator(theme='fast', disabled=True, show_index=False)
poly_grid = pn.widgets.Tabulator(theme='fast', disabled=True, show_index=False)

data_tabs = pn.Tabs(
    ("📰 Breaking News (Reuters)", news_grid),
    ("📈 Prediction Markets (Polymarket)", poly_grid),
    sizing_mode="stretch_both",
    min_height=300
)

## -- Polling logic
cached_layers = {}
cached_counts = {}

## -- State Cache for Map Layers
# This remembers the most recent data so it doesn't vanish between Rust polls!
cached_layers = {}
cached_counts = {}

def update_dashboard():
    ''' Polling loop '''
    data = engine.poll()
    
    # -- Update cache if new data arrives)
    # ✈️ Flights (Green)
    if "opensky" in data:
        df = pl.from_arrow(data["opensky"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["opensky"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["opensky"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[57, 255, 20, 200], get_radius=5000, radius_min_pixels=2)

    # 🚢 Ships (Blue)
    if "ais_stream" in data:
        df = pl.from_arrow(data["ais_stream"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["ais_stream"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["ais_stream"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[0, 150, 255, 200], get_radius=4000, radius_min_pixels=2)

    # 🌋 Earthquakes (Orange)
    if "usgs" in data:
        df = pl.from_arrow(data["usgs"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["usgs"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["usgs"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 165, 0, 200], get_radius=15000, radius_min_pixels=5)

    # 🔥 Wildfires (Yellow)
    if "nasa_eonet" in data:
        df = pl.from_arrow(data["nasa_eonet"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["nasa_eonet"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["nasa_eonet"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 255, 0, 200], get_radius=20000, radius_min_pixels=6)

    # ⚔️ Conflicts (Red)
    if "acled" in data:
        df = pl.from_arrow(data["acled"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["acled"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["acled"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 50, 50, 200], get_radius=10000, radius_min_pixels=4)


    # -- Build map from cache + switches
    
    active_layers = []
    total_tracked = 0

    if switch_flights.value and "opensky" in cached_layers:
        active_layers.append(cached_layers["opensky"])
        total_tracked += cached_counts["opensky"]
        
    if switch_ships.value and "ais_stream" in cached_layers:
        active_layers.append(cached_layers["ais_stream"])
        total_tracked += cached_counts["ais_stream"]
        
    if switch_quakes.value and "usgs" in cached_layers:
        active_layers.append(cached_layers["usgs"])
        total_tracked += cached_counts["usgs"]
        
    if switch_fires.value and "nasa_eonet" in cached_layers:
        active_layers.append(cached_layers["nasa_eonet"])
        total_tracked += cached_counts["nasa_eonet"]
        
    if switch_acled.value and "acled" in cached_layers:
        active_layers.append(cached_layers["acled"])
        total_tracked += cached_counts["acled"]

    # Push to GPU
    interactive_map.layers = active_layers

    # Update Data Grids (You can cache these too if they blink!)
    if "google_news_reuters" in data:
        df = pl.from_arrow(data["google_news_reuters"])
        if len(df) > 0:
            news_grid.value = df.to_pandas() 
            
    if "polymarket" in data:
        df = pl.from_arrow(data["polymarket"])
        if len(df) > 0:
            poly_grid.value = df.sort("volume", descending=True).to_pandas()

    if total_tracked > 0:
        system_stats.object = f"🟢 **Engine Status:** Online\n📡 **Tracking:** {total_tracked:,} global events"

pn.state.add_periodic_callback(update_dashboard, period=5000)


## -- Master Template Layout
template = pn.template.FastListTemplate(
    title="OSINTxpress Global Monitor",
    theme="dark",
    header_background="#0d1117",
    sidebar_width=240,
    sidebar=[
        system_stats,
        pn.layout.Divider(),
        layer_controls
    ],
    main=[
        pn.Row(map_pane, sizing_mode="stretch_both"),
        pn.Row(data_tabs, sizing_mode="stretch_both")
    ]
)

template.servable()