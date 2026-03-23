import warnings
warnings.filterwarnings('ignore', message='No CRS exists on data')

import polars as pl
import panel as pn
import arro3.core as ac # <-- Using arro3 natively!
import lonboard
from lonboard.basemap import MaplibreBasemap, CartoStyle
from geoarrow.rust.core import points 

from osintxpress import OsintEngine, SourceAdapter

engine = OsintEngine(worker_threads=2)
engine.add_source(
    name='flights', # Optional name
    adapter=SourceAdapter.OPENSKY,
    poll_interval_sec=10 
)
engine.start_all()

# Start an empty Lonboard WebGL map
interactive_map = lonboard.Map(
    layers=[], 
    basemap=MaplibreBasemap(style=CartoStyle.DarkMatter), 
    view_state={'longitude': 34.7, 'latitude': 31.5, 'zoom': 5, 'pitch': 45}
)

stats = pn.pane.Markdown('### Awaiting signal...', styles={'color': '#39FF14'})

def update_dashboard():
    data = engine.poll()
    
    # 1. No len() check on the raw arro3 batch!
    if 'flights' in data:
        # 2. Convert to Polars first
        df = pl.from_arrow(data['flights']).drop_nulls(subset=['longitude', 'latitude'])
        
        # 3. Check len() on the DataFrame
        if len(df) > 0:
            # 4. Crush the Polars streams into contiguous Arro3 arrays
            lon_arr = ac.ChunkedArray.from_arrow(df['longitude']).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df['latitude']).combine_chunks()
            
            # 5. Feed contiguous arrays to geoarrow-rust
            geometry_col = points([lon_arr, lat_arr])
            
            # 6. Append natively to an Arro3 Table
            table = ac.Table.from_arrow(df).append_column('geometry', geometry_col)
            
            # Stream the GeoArrow binary to the browser GPU
            new_layer = lonboard.ScatterplotLayer(
                table=table,
                get_fill_color=[57, 255, 20, 200], # Fancy glowy Green
                get_radius=4000,
                radius_min_pixels=2
            )
            
            # Overwrite the map layers
            interactive_map.layers = [new_layer]
            stats.object = f'### Tracking **{len(df)}** aircraft'

# Poll every 5 seconds
pn.state.add_periodic_callback(update_dashboard, period=5000)


# Render flights view
pn.Column(
    stats,
    pn.pane.IPyWidget(interactive_map, sizing_mode='stretch_both', min_height=700),
    styles={'background': '#121212', 'padding': '20px'}
).servable()