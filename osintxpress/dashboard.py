import os, warnings, threading, json
from datetime import datetime

import arro3.core as ac
import polars as pl
from geoarrow.rust.core import points 

import panel as pn
import lonboard
from lonboard.basemap import MaplibreBasemap, CartoStyle

from osintxpress import OsintEngine, SourceAdapter, login_telegram


warnings.filterwarnings("ignore", message="No CRS exists on data")
pn.extension('ipywidgets', 'tabulator', notifications=True, sizing_mode="stretch_width")


# ==========================================
# 1. ENGINE SETUP & PERSISTENCE
# ==========================================
engine = OsintEngine(worker_threads=4)

stealth_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

engine.add_rest_source(SourceAdapter.OPENSKY, poll_interval_sec=5)
engine.add_rest_source(SourceAdapter.USGS, poll_interval_sec=15)
engine.add_rest_source(SourceAdapter.NASA_EONET, poll_interval_sec=30)
engine.add_rest_source(SourceAdapter.ACLED, poll_interval_sec=60)
engine.add_rest_source(SourceAdapter.GOOGLE_NEWS_REUTERS, poll_interval_sec=60, headers=stealth_headers)
engine.add_rest_source(SourceAdapter.POLYMARKET, poll_interval_sec=30, headers=stealth_headers)

AIS_API_KEY = os.getenv("AIS_API_KEY", "YOUR_AIS_API_KEY") 
ais_payload = json.dumps({
    "APIKey": AIS_API_KEY,
    "BoundingBoxes": [[[-90, -180], [90, 180]]], 
    "FilterMessageTypes": ["PositionReport"]
})
engine.add_ws_source(SourceAdapter.AIS_STREAM, init_message=ais_payload)

TG_API_ID = int(os.getenv("TG_API_ID", 123456))
TG_API_HASH = os.getenv("TG_API_HASH", "API_HASH") 
SESSION = "osint.session"
CHANNELS_FILE = "telegram_sources.json"

if os.path.exists(CHANNELS_FILE):
    with open(CHANNELS_FILE, "r") as f:
        active_channels = json.load(f)
else:
    active_channels = ["abualiexpress", "shiranraz"]

for channel in active_channels:
    engine.add_telegram_source(
        name=f"tg_{channel}", 
        target_channel=channel, 
        tg_api_id=TG_API_ID,
        tg_api_hash=TG_API_HASH,
        tg_session_path=SESSION)

engine.start_all()

# Dummy method so the UI button doesn't crash until we update the Rust backend!
if not hasattr(engine, 'fetch_telegram_history'):
    def dummy_fetch(channel, limit):
        print(f"Rust backend needs MPSC channel update to fetch {limit} msgs from {channel}!")
    engine.fetch_telegram_history = dummy_fetch

# ==========================================
# 2. MAP UI COMPONENTS
# ==========================================
switch_flights = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_ships = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_quakes = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_fires = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_acled = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))

layer_controls = pn.Column(
    "### 🗺️ Active Layers",
    pn.Row(switch_flights, pn.pane.HTML("<b style='color: rgb(57, 255, 20); font-size: 1.1em;'>✈️ Flights</b>"), align='center'),
    pn.Row(switch_ships, pn.pane.HTML("<b style='color: rgb(0, 150, 255); font-size: 1.1em;'>🚢 Maritime</b>"), align='center'),
    pn.Row(switch_quakes, pn.pane.HTML("<b style='color: rgb(255, 165, 0); font-size: 1.1em;'>🌋 Quakes</b>"), align='center'),
    pn.Row(switch_fires, pn.pane.HTML("<b style='color: rgb(255, 255, 0); font-size: 1.1em;'>🔥 Wildfires</b>"), align='center'),
    pn.Row(switch_acled, pn.pane.HTML("<b style='color: rgb(255, 50, 50); font-size: 1.1em;'>⚔️ Conflicts</b>"), align='center'),
)

system_stats = pn.pane.Markdown("🟢 **Engine Status:** Online\n📡 **Tracking:** 0 events")

interactive_map = lonboard.Map(
    layers=[], 
    basemap=MaplibreBasemap(style=CartoStyle.DarkMatter), 
    view_state={"longitude": 34.8, "latitude": 31.5, "zoom": 3, "pitch": 25, "minZoom": 2.5, "maxZoom": 20}
)

map_pane = pn.pane.IPyWidget(interactive_map, sizing_mode="stretch_both")

map_tab = pn.Row(
    pn.Column(system_stats, pn.layout.Divider(), layer_controls, width=180),
    map_pane,
    sizing_mode="stretch_both",
    styles={'height': '85vh'} 
)

# ==========================================
# 3. TELEGRAM UI & HISTORY CONTROLS
# ==========================================
feed_container = pn.Column(sizing_mode="stretch_width")

# 3a. Active Channels List
active_channels_pane = pn.Column()
def update_channels_ui():
    blocks = [
        pn.pane.HTML(f"""
        <div style='padding: 6px; border-left: 3px solid #0096ff; margin-bottom: 8px; background: var(--neutral-fill-rest); border-radius: 0 4px 4px 0;'>
            <b>@{ch}</b>
        </div>
        """) for ch in active_channels
    ]
    active_channels_pane.objects = blocks

update_channels_ui()

# 3b. History Fetcher
history_select = pn.widgets.Select(name="Target Channel", options=active_channels)
history_limit = pn.widgets.IntInput(name="Messages to Fetch", value=5, start=1, end=100)
history_btn = pn.widgets.Button(name="⏪ Fetch History", button_type="primary")


def trigger_history_fetch(event):

    target = history_select.value
    limit = history_limit.value
    engine.fetch_telegram_history(target, limit)
    pn.state.notifications.info(f"Rust command dispatched: Fetching {limit} messages from @{target}...")

history_btn.on_click(trigger_history_fetch)

telegram_sidebar = pn.Column(
    pn.pane.Markdown("### 📡 Registered Sources"),
    active_channels_pane,
    pn.layout.Divider(),
    pn.pane.Markdown("### ⏪ Historical Lookup"),
    history_select,
    history_limit,
    history_btn,
    width=240
)

telegram_tab = pn.Row(
    telegram_sidebar,
    pn.Column(feed_container, scroll=True, sizing_mode="stretch_both"),    
    sizing_mode="stretch_both",
    styles={'height': '85vh'} 
)


# ==========================================
# 4. DATA GRIDS (WITH EXPANDABLE ROWS!)
# ==========================================
def expand_news(row):
    title = row.get("title", "No Title")
    link = row.get("link", "#")
    pub = row.get("pubDate", row.get("date", "Unknown Date"))
                
    return pn.pane.HTML(f"""
        <div style='padding: 15px; margin: 5px; background-color: var(--neutral-fill-rest); border-left: 4px solid #0096ff; border-radius: 4px; cursor: default;'>
            <small style='color: gray;'>{pub}</small><br><br>
            <span style='font-size: 1.1em;'><b>{title}</b></span><br><br>
            <a href='{link}' target='_blank' style='color: #0096ff; text-decoration: none;'><b>Read Full Article in New Tab 🔗</b></a>
        </div>
    """, sizing_mode="stretch_width")

news_grid = pn.widgets.Tabulator(theme='fast', show_index=False, row_content=expand_news, selectable=False)
poly_grid = pn.widgets.Tabulator(theme='fast', disabled=True, show_index=False)

def toggle_row_expansion(event):
    if event.row in news_grid.expanded:
        news_grid.expanded = [r for r in news_grid.expanded if r != event.row]
    else:
        news_grid.expanded = news_grid.expanded + [event.row]

news_grid.on_click(toggle_row_expansion)

data_tabs = pn.Tabs(
    ("📰 Breaking News", news_grid),
    ("📈 Polymarket", poly_grid),
    sizing_mode="stretch_both"
)


# ==========================================
# 5. DYNAMIC ENGINE CONFIG & GUI LOGIN
# ==========================================
config_desc = pn.pane.Markdown("### ⚙️ Hot-Swap Data Sources\nInject new REST APIs, WebSockets, or Telegram channels into the running Rust engine without restarting.")

adapter_select = pn.widgets.Select(name="Parser Module", options=[
    "Telegram Channel", "REST: NASA EONET", "REST: ACLED", "REST: Polymarket", "REST: Reuters News"
])
target_input = pn.widgets.TextInput(name="Target Channel / URL", placeholder="e.g. intel_slava")
add_source_btn = pn.widgets.Button(name="⚡ Inject into Engine", button_type="primary", margin=(20, 5, 5, 5))

def hot_swap_source(event):
    target = target_input.value.strip()
    sel = adapter_select.value
    
    if sel == "Telegram Channel" and target:
        engine.add_telegram_source(
            name=f"tg_{target}", 
            target_channel=target, 
            tg_api_id=TG_API_ID, 
            tg_api_hash=TG_API_HASH,
            tg_session_path=SESSION)
        
        if target not in active_channels:
            active_channels.append(target)
            with open(CHANNELS_FILE, "w") as f:
                json.dump(active_channels, f)
            # Update the UI dynamically!
            update_channels_ui()
            history_select.options = active_channels
            
        pn.state.notifications.success(f"Successfully multiplexed Telegram channel: @{target}")
        
    elif sel == "REST: NASA EONET":
        engine.add_rest_source(SourceAdapter.NASA_EONET, poll_interval_sec=30)
        pn.state.notifications.success("NASA EONET pipeline started!")
        switch_fires.value = True
    elif sel == "REST: ACLED":
        engine.add_rest_source(SourceAdapter.ACLED, poll_interval_sec=60)
        pn.state.notifications.success("ACLED Conflict pipeline started!")
        switch_acled.value = True
    elif sel == "REST: Reuters News":
        engine.add_rest_source(SourceAdapter.GOOGLE_NEWS_REUTERS, poll_interval_sec=60, headers=stealth_headers)
        pn.state.notifications.success("Reuters News pipeline started!")
    elif sel == "REST: Polymarket":
        engine.add_rest_source(SourceAdapter.POLYMARKET, poll_interval_sec=30, headers=stealth_headers)
        pn.state.notifications.success("Polymarket pipeline started!")
        
    target_input.value = ""

add_source_btn.on_click(hot_swap_source)

# ---- GUI TELEGRAM LOGIN ----
auth_event = threading.Event()
tg_login_btn = pn.widgets.Button(name="🔐 Authorize Telegram Session", button_type="success", width=250)

api_id_input = pn.widgets.TextInput(name="API ID")
api_hash_input = pn.widgets.PasswordInput(name="API Hash")
phone_input = pn.widgets.TextInput(name="Phone Number", placeholder="+1234567890")

request_code_btn = pn.widgets.Button(name="1. Request Code", button_type="primary")
code_input = pn.widgets.TextInput(name="2FA Code", disabled=True, placeholder="Waiting for Telegram...")
submit_code_btn = pn.widgets.Button(name="2. Submit Code", button_type="success", disabled=True)

def auto_submit_code(event):
    code = event.new.strip()
    if len(code) == 5 and code.isdigit():
        auth_event.set()

code_input.param.watch(auto_submit_code, 'value')

def get_code_callback():
    def update_ui():
        code_input.disabled = False
        submit_code_btn.disabled = False
        request_code_btn.disabled = True
        pn.state.notifications.info("Please enter the 5-digit code sent to your Telegram app.")
    
    pn.state.execute(update_ui)
    auth_event.wait()
    return code_input.value.strip()

def start_auth_process(event):
    if not api_id_input.value or not api_hash_input.value or not phone_input.value:
        pn.state.notifications.error("API ID, Hash, and Phone are required!")
        return
        
    auth_event.clear()
    code_input.value = ""
    request_code_btn.disabled = True
    request_code_btn.name = "Requesting..."
    
    def worker():
        try:
            login_telegram(int(api_id_input.value), api_hash_input.value, phone_input.value, SESSION, get_code_callback)
            def on_success():
                pn.state.notifications.success("Authorization complete!")
                template.close_modal()
            pn.state.execute(on_success)
        except Exception as e:
            error_msg = str(e)
            pn.state.execute(lambda msg=error_msg: pn.state.notifications.error(f"Login failed: {msg}"))
        finally:
            pn.state.execute(reset_login_ui)
            
    threading.Thread(target=worker, daemon=True).start()

def reset_login_ui():
    request_code_btn.name = "1. Request Code"
    request_code_btn.disabled = False
    code_input.disabled = True
    submit_code_btn.disabled = True

def submit_code(event):
    if code_input.value.strip():
        auth_event.set()
    else:
        pn.state.notifications.error("Please enter the code first.")

request_code_btn.on_click(start_auth_process)
submit_code_btn.on_click(submit_code)

login_instructions = pn.pane.Markdown("""
### 🔐 Telegram MTProto Authorization
**How to get your API credentials:**
1. Go to [my.telegram.org](https://my.telegram.org) and log in.
2. Click **API development tools**.
3. Fill out the form (App title: `OsintXpress`, Short name: `osintxpress`, Platform: `Desktop`).
4. Click **Create application**.
5. Paste your new **api_id** and **api_hash** below.
""")

login_modal = pn.Column(
    login_instructions,
    pn.Row(api_id_input, api_hash_input),
    phone_input,
    request_code_btn,
    pn.layout.Divider(),
    pn.Row(code_input, submit_code_btn)
)

def show_login_modal(event):
    template.modal[0].clear()
    template.modal[0].append(login_modal)
    template.open_modal()
    
tg_login_btn.on_click(show_login_modal)

config_tab = pn.Column(
    config_desc, 
    pn.Row(adapter_select, target_input, add_source_btn),
    pn.layout.Divider(),
    pn.pane.Markdown("### 🔑 Authentication"),
    tg_login_btn, 
    sizing_mode="stretch_both"
)

# ==========================================
# 6. MASTER TEMPLATE ASSEMBLY
# ==========================================
sticky_tab_style = """
:host { height: 100%; }
.bk-header {
    position: sticky;
    top: 0;
    z-index: 1000;
    background-color: var(--design-background-color, #181818);
    border-bottom: 1px solid #333;
    display: flex;
    justify-content: center !important; 
}
"""

master_tabs = pn.Tabs(
    ("🗺️ Tactical Map", map_tab),
    ("📱 Live Telegram", telegram_tab), # Using the new layout here!
    ("📊 Data Feeds", data_tabs),
    ("⚙️ Engine Config", config_tab),
    dynamic=False, 
    sizing_mode="stretch_both",
    stylesheets=[sticky_tab_style]
)

def create_tg_card(channel, text, timestamp):
    
    dt = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
    color = "var(--accent-fill-active)" if "abu" in channel.lower() else "var(--neutral-fill-active)"
    
    text = text.replace('\\n', '\n')
    parts = text.split('\n', 1)
    if len(parts) > 1:
        formatted_text = f"<b style='font-size: 1.15em;'>{parts[0]}</b><br><br>{parts[1].replace('\n', '<br>')}"
    else:
        formatted_text = text.replace('\n', '<br>')

    return pn.pane.HTML(
        f"""
        <div style="border-left: 4px solid {color}; padding-left: 10px; margin-bottom: 10px; background: var(--neutral-fill-rest);">
            <small style="color: gray;"><b>@{channel}</b> • {dt}</small><br>
            <div style="direction: rtl; text-align: right; font-family: sans-serif;">{formatted_text}</div>
        </div>
        """,
        sizing_mode="stretch_width"
    )

# ==========================================
# 7. POLLING LOGIC
# ==========================================
cached_layers = {}
cached_counts = {}

def update_dashboard():
    data = engine.poll()
    
    if "opensky" in data:
        df = pl.from_arrow(data["opensky"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["opensky"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["opensky"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[57, 255, 20, 200], get_radius=5000, radius_min_pixels=2)

    if "ais_stream" in data:
        df = pl.from_arrow(data["ais_stream"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["ais_stream"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["ais_stream"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[0, 150, 255, 200], get_radius=4000, radius_min_pixels=2)

    if "usgs" in data:
        df = pl.from_arrow(data["usgs"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["usgs"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["usgs"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 165, 0, 200], get_radius=15000, radius_min_pixels=5)

    if "nasa_eonet" in data:
        df = pl.from_arrow(data["nasa_eonet"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["nasa_eonet"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["nasa_eonet"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 255, 0, 200], get_radius=20000, radius_min_pixels=6)

    if "acled" in data:
        df = pl.from_arrow(data["acled"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["acled"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            cached_layers["acled"] = lonboard.ScatterplotLayer(table=table, get_fill_color=[255, 50, 50, 200], get_radius=10000, radius_min_pixels=4)

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

    interactive_map.layers = active_layers

    # -- Update Data Grids --
    if "google_news_reuters" in data:
        df = pl.from_arrow(data["google_news_reuters"])
        if len(df) > 0:
            news_grid.value = df.to_pandas() 
            
    if "polymarket" in data:
        df = pl.from_arrow(data["polymarket"])
        if len(df) > 0:
            poly_grid.value = df.sort("volume", descending=True).to_pandas()


    # -- Update Telegram --
    new_messages = []
    for tg_src in list(data.keys()): 
        if tg_src.startswith("tg_"):
            # Convert to Polars FIRST!
            df = pl.from_arrow(data[tg_src])
            
            # Now we can safely check the length
            if len(df) > 0:
                df = df.sort("date", descending=False)
                for row in df.iter_rows(named=True):
                    new_messages.append(create_tg_card(row['channel'], row['text'], row['date']))
                
    if new_messages:
        feed_container.objects = new_messages + feed_container.objects[:100]

    if total_tracked > 0:
        system_stats.object = f"🟢 **Engine Status:** Online\n📡 **Tracking:** {total_tracked:,} events"

pn.state.add_periodic_callback(update_dashboard, period=5000)

template = pn.template.FastListTemplate(
    title="OSINTxpress Global Monitor",
    theme="dark",
    header_background="#0d1117",
    main=[master_tabs],
    main_layout=None 
)

template.modal.append(pn.Column())
template.servable()