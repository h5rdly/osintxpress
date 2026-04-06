import os, sys, warnings, threading, json, io, base64, atexit, urllib.request
from datetime import datetime

import arro3.core as ac
import polars as pl
from geoarrow.rust.core import points 

import panel as pn
import lonboard
from lonboard.basemap import MaplibreBasemap, CartoStyle

# Added scrape_article to our Rust imports!
from osintxpress import (OsintEngine, SourceAdapter, login_telegram, scrape_article, 
    fetch_submarine_cables, _is_rtl)

warnings.filterwarnings("ignore", message="No CRS exists on data")
pn.extension('ipywidgets', 'tabulator', 'terminal', notifications=True, sizing_mode="stretch_width")


class DualLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

# Intercept all print() commands and Python errors
sys.stdout = DualLogger("osint.log")
sys.stderr = sys.stdout


## -- Engine setup
##

engine = OsintEngine(worker_threads=4)

stealth_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

engine.add_rest_source(SourceAdapter.OPENSKY, poll_interval_sec=5)
engine.add_rest_source(SourceAdapter.CELESTRAK, poll_interval_sec=5)
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


## --  UI components & Static Map Layers
##

base_layers = []

switch_flights = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_satellites = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0)) 
switch_ships = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_quakes = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_fires = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))
switch_acled = pn.widgets.Switch(value=True, margin=(5, 10, 5, 0))

layer_controls = pn.Column(
    "### 🗺️ Active Layers",
    pn.Row(switch_flights, pn.pane.HTML("<b style='color: rgb(57, 255, 20); font-size: 1.1em;'>✈️ Flights</b>"), align='center'),
    pn.Row(switch_satellites, pn.pane.HTML("<b style='color: rgb(255, 255, 255); font-size: 1.1em;'>🛰️ Satellites</b>"), align='center'),
    pn.Row(switch_ships, pn.pane.HTML("<b style='color: rgb(0, 150, 255); font-size: 1.1em;'>🚢 Maritime</b>"), align='center'),
    pn.Row(switch_quakes, pn.pane.HTML("<b style='color: rgb(255, 165, 0); font-size: 1.1em;'>🌋 Quakes</b>"), align='center'),
    pn.Row(switch_fires, pn.pane.HTML("<b style='color: rgb(255, 255, 0); font-size: 1.1em;'>🔥 Wildfires</b>"), align='center'),
    pn.Row(switch_acled, pn.pane.HTML("<b style='color: rgb(255, 50, 50); font-size: 1.1em;'>⚔️ Conflicts</b>"), align='center'),
)

system_stats = pn.pane.Markdown("🟢 **Engine Status:** Online\n📡 **Tracking:** 0 events")

interactive_map = lonboard.Map(
    layers=base_layers, # Initialize with the submarine cables
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


## -- Connection Manager UI
##

def create_row_callbacks(src_name, toggle_widget):

    def on_toggle(event):
        if event.new:
            engine.start_source(src_name)
            pn.state.notifications.success(f"▶️ {src_name.upper()} started")
        else:
            engine.stop_source(src_name)
            pn.state.notifications.warning(f"⏸️ {src_name.upper()} stopped")


    def on_interval_change(event):

        new_val = event.new
        c = engine.source_registry[src_name]
        c['interval'] = new_val
        
        # Hot-swap the Tokio task
        engine.stop_source(src_name)
        engine.add_rest_source(c['adapter'], poll_interval_sec=new_val, headers=c['headers'])
        
        if not toggle_widget.value:
            engine.stop_source(src_name)
            
        pn.state.notifications.info(f"⏱️ {src_name.upper()} interval updated to {new_val}s")

    return on_interval_change, on_toggle


def build_source_manager():

    rows = [
        pn.pane.Markdown("### 📡 Active Connection Manager\n*Live control over the Rust background polling workers.*"),
        pn.Row(pn.pane.Markdown("**Source Name**", width=180), pn.pane.Markdown("**Poll**", width=80), pn.pane.Markdown("**Status**", width=60)),
        pn.layout.Divider()
    ]
    
    # Read from the Engine's internal Python state
    for name, cfg in engine.source_registry.items():
        badge_color = "#0096ff" if cfg['type'] == 'ws' else "#39ff14"
        badge_text = "WS" if cfg['type'] == 'ws' else "REST"
        
        label = pn.pane.HTML(f"<b style='font-size:1.1em;'>{name.upper()}</b> <span style='background:{badge_color}; color:black; padding:2px 6px; border-radius:4px; font-size:0.7em;'>{badge_text}</span>", width=180)
        
        toggle = pn.widgets.Switch(value=True, width=50, margin=(10,0))
        interval_change_cb, toggle_cb = create_row_callbacks(name, toggle)
        toggle.param.watch(toggle_cb, 'value')
        
        if cfg['type'] == 'rest':
            spinner = pn.widgets.IntInput(value=cfg['interval'], start=1, end=3600, width=80)
            spinner.param.watch(interval_change_cb, 'value')
        else:
            spinner = pn.pane.Markdown("*(Push)*", width=80, margin=(10,0))
        
        rows.append(pn.Row(label, spinner, toggle, align="center"))
        
    return pn.Column(*rows, sizing_mode='stretch_width')

source_manager_panel = build_source_manager()


## -- Telegram UI / History / Login
##  

def create_tg_card(channel, text, timestamp, media_path=""):

    dt = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
    color = "var(--accent-fill-active)" if "abu" in channel.lower() else "var(--neutral-fill-active)"
    
    direction = "rtl" if _is_rtl(text) else "ltr"
    align = "right" if direction == "rtl" else "left"
    
    text = text.replace('\\n', '\n')
    parts = text.split('\n', 1)
    if len(parts) > 1:
        formatted_text = f"<b style='font-size: 1.15em;'>{parts[0]}</b><br><br>{parts[1].replace('\n', '<br>')}"
    else:
        formatted_text = text.replace('\n', '<br>')

    # 📸 Wrap the local file in an HTML <details> collapsible!
    img_html = ""
    if media_path and os.path.exists(media_path):
        try:
            with open(media_path, "rb") as img_file:
                b64_str = base64.b64encode(img_file.read()).decode('utf-8')
                
                # Native HTML collapsible: no Python callbacks required!
                img_html = f"""
                <details style="margin-top: 12px;">
                    <summary style="cursor: pointer; color: {color}; font-weight: bold; outline: none; user-select: none; font-size: 0.9em;">
                        📎 Show Attached Media
                    </summary>
                    <img src='data:image/jpeg;base64,{b64_str}' style='max-width: 100%; border-radius: 4px; margin-top: 8px;'>
                </details>
                """
        except Exception as e:
            print(f"Error loading image {media_path}: {e}")

    return pn.pane.HTML(
        f"""
        <div style="border-left: 4px solid {color}; padding-left: 10px; padding-bottom: 5px; margin-bottom: 10px; background: var(--neutral-fill-rest);">
            <small style="color: gray;"><b>@{channel}</b> • {dt}</small><br>
            <div style="direction: {direction}; text-align: {align}; font-family: sans-serif;">
                {formatted_text}
                {img_html}
            </div>
        </div>
        """,
        sizing_mode="stretch_width"
    )


feed_container = pn.Column(sizing_mode="stretch_width")

# Multi-Select List
sources_list = pn.widgets.MultiSelect(
    name='Registered Sources (Ctrl/Shift-click to multi-select)',
    options=active_channels,
    size=10, 
    sizing_mode='stretch_width'
)

# -  New Channel Controls
new_channel_input = pn.widgets.TextInput(placeholder="e.g. ReutersWorldChannel", sizing_mode='stretch_width')
add_channel_btn = pn.widgets.Button(name="➕ Add Channel", button_type="success", sizing_mode='stretch_width')

def on_add_channel(event):

    new_channel = new_channel_input.value.strip()
    if new_channel:
        # Add to Rust engine
        engine.add_telegram_source(
            name=f"tg_{new_channel}", 
            target_channel=new_channel, 
            tg_api_id=TG_API_ID, 
            tg_api_hash=TG_API_HASH,
            tg_session_path=SESSION
        )
        
        # Update active_channels and persist to disk
        if new_channel not in active_channels:
            active_channels.append(new_channel)
            with open(CHANNELS_FILE, "w") as f:
                json.dump(active_channels, f)
            
            # Update the UI list options
            sources_list.options = active_channels.copy()
            
        new_channel_input.value = ""
        pn.state.notifications.success(f"Multiplexed Telegram channel: @{new_channel}")

add_channel_btn.on_click(on_add_channel)

# - History Controls
fetch_limit_input = pn.widgets.IntInput(name="Messages", value=5, start=1, end=1000, width=80)
fetch_history_btn = pn.widgets.Button(name="⏪ Fetch History", button_type="primary", sizing_mode='stretch_width')

def on_fetch_history(event):
    selected_sources = sources_list.value
    limit = fetch_limit_input.value
    
    if not selected_sources:
        pn.state.notifications.warning("No sources selected!")
        return
        
    for target_channel in selected_sources:
        engine.fetch_telegram_history(target_channel, limit) 
        pn.state.notifications.info(f"Fetching {limit} historical messages from @{target_channel}...")

fetch_history_btn.on_click(on_fetch_history)

# - Telegram login

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

# Collapsible Sidebar 
toggle_sidebar_btn = pn.widgets.Button(name='◀', width=40, button_type='default')

telegram_sidebar_controls = pn.Column(
    sources_list,
    pn.layout.Divider(),
    fetch_limit_input, 
    fetch_history_btn,
    pn.layout.Divider(),
    new_channel_input, 
    add_channel_btn,
    pn.pane.Markdown("### 🔑 Authentication"),
    tg_login_btn, 
    width=260
)

def toggle_sidebar(event):

    if telegram_sidebar_controls.visible:
        telegram_sidebar_controls.visible = False
        toggle_sidebar_btn.name = '▶'
        toggle_sidebar_btn.width = 40 
    else:
        telegram_sidebar_controls.visible = True
        toggle_sidebar_btn.name = '◀'
        toggle_sidebar_btn.width = 40

toggle_sidebar_btn.on_click(toggle_sidebar)

telegram_sidebar = pn.Column(
    toggle_sidebar_btn,
    telegram_sidebar_controls,
    sizing_mode='stretch_height' 
)

telegram_tab = pn.Row(
    telegram_sidebar,
    pn.Column(feed_container, scroll=True, sizing_mode="stretch_both"),    
    sizing_mode="stretch_both",
    styles={'height': '85vh'} 
)


## -- Data Grids
##

def expand_news(row):
    title = row.get("title", "No Title")
    link = row.get("link", "#")
    pub = row.get("pubDate", row.get("date", "Unknown Date"))
    
    # 🚀 Dynamically scrape the article via Rust when expanded
    article_markdown = scrape_article(link)

    # formatted_text = article_text.replace("\n", "<br>")
                
    return pn.Column(
        pn.pane.HTML(f"""
            <div style='padding: 15px 15px 0 15px; margin-top: 5px; background-color: var(--neutral-fill-rest); border-left: 4px solid #0096ff; border-radius: 4px 4px 0 0; cursor: default;'>
                <small style='color: gray;'>{pub}</small><br><br>
                <span style='font-size: 1.1em;'><b>{title}</b></span><br>
                <hr style='border: 1px solid #333; margin: 15px 0 0 0;'>
            </div>
        """, sizing_mode="stretch_width"),
        pn.pane.Markdown(
            article_markdown, 
            sizing_mode="stretch_width", 
            styles={'background-color': 'var(--neutral-fill-rest)', 'padding': '0 15px', 'border-left': '4px solid #0096ff', 'font-family': 'serif', 'font-size': '1.05em'}
        ),
        pn.pane.HTML(f"""
            <div style='padding: 0 15px 15px 15px; margin-bottom: 5px; background-color: var(--neutral-fill-rest); border-left: 4px solid #0096ff; border-radius: 0 0 4px 4px; cursor: default;'>
                <br>
                <a href='{link}' target='_blank' style='color: #0096ff; text-decoration: none;'><b>View Original Source 🔗</b></a>
            </div>
        """, sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
        margin=0
    )
    
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


## --  Log Viewer
##

log_btn = pn.widgets.Button(
    name='📄', 
    width=40, 
    height=40, 
    sizing_mode='fixed', 
    button_type='default', # Blends better with the dark header
    margin=(10, 15),
    align='center'
)

log_terminal = pn.widgets.Terminal(
    "Waiting for logs...\n",
    options={"cursorBlink": False, "theme": {"background": "#0d1117"}},
    height=500, sizing_mode='stretch_width'
)

log_modal = pn.Column(
    pn.pane.Markdown("### 📜 System Terminal\n*Live feed from `osint.log`*"),
    log_terminal,
    sizing_mode='stretch_both'
)

def show_logs(event):

    template.modal[0].clear()
    template.modal[0].append(log_modal)
    template.open_modal()
    tail_logs()

log_btn.on_click(show_logs)


_log_position = os.path.getsize("osint.log") if os.path.exists("osint.log") else 0

def tail_logs():

    global _log_position
    
    # Only read the disk if the modal is open
    if len(template.modal[0].objects) > 0 and template.modal[0].objects[0] == log_modal:
        if os.path.exists("osint.log"):
            
            # Handle the case where you delete the log file to clear it
            current_size = os.path.getsize("osint.log")
            if current_size < _log_position:
                _log_position = 0 
                log_terminal.clear()
            
            with open("osint.log", "r", encoding="utf-8", errors="replace") as f:
                f.seek(_log_position)
                new_text = f.read()
                if new_text:
                    # xterm.js (which powers Panel's terminal) requires \r\n for line breaks
                    new_text = new_text.replace('\n', '\r\n').replace('\r\r\n', '\r\n')
                    log_terminal.write(new_text)
                    _log_position = f.tell()

pn.state.add_periodic_callback(tail_logs, period=1000)


## -- Engine config & login
##

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

config_tab = pn.Column(
    source_manager_panel,
    config_desc, 
    pn.layout.Divider(),
    pn.Row(adapter_select, target_input, add_source_btn),
    pn.layout.Divider(),
    sizing_mode="stretch_both"
)

## -- Master template
##

sticky_tab_style = """
:host { height: 100%; }
.bk-header {
    position: sticky;
    top: 0;
    z-index: 5;
    background-color: var(--design-background-color, #181818);
    border-bottom: 1px solid #333;
    display: flex;
    justify-content: center !important; 
}
"""

master_tabs = pn.Tabs(
    ("🗺️ Tactical Map", map_tab),
    ("📱 Live Telegram", telegram_tab), 
    ("📊 Data Feeds", data_tabs),
    ("⚙️ Engine Config", config_tab),
    dynamic=False, 
    sizing_mode="stretch_both",
    stylesheets=[sticky_tab_style]
)


## -- Background Initializers
## 

def load_submarine_cables():
    
    cable_url = "https://raw.githubusercontent.com/telegeography/www.submarinecablemap.com/master/web/public/api/v3/cable/cable-geo.json"
    try:
        print("#### Fetching submarine cable network in the background...")
        cable_table = fetch_submarine_cables()
        
        cable_layer = lonboard.PathLayer(
            table=cable_table,
            get_color=[0, 255, 255, 100], 
            width_min_pixels=1.5
        )
        base_layers.append(cable_layer)
        
        # Merge the new base layer with whatever active layers are currently showing
        interactive_map.layers = base_layers + list(interactive_map.layers)
        print("#### Submarine cables loaded.")
    except Exception as e:
        print(f"Could not load undersea cables: {e}")

# Fire this function exactly once, 500ms after the dashboard fully loads!
# pn.state.add_periodic_callback(load_submarine_cables, period=500, count=1)


## -- Polling logic
##

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

    if "celestrak" in data:
        df = pl.from_arrow(data["celestrak"]).drop_nulls(subset=["longitude", "latitude"])
        if len(df) > 0:
            cached_counts["celestrak"] = len(df)
            lon_arr = ac.ChunkedArray.from_arrow(df["longitude"]).combine_chunks()
            lat_arr = ac.ChunkedArray.from_arrow(df["latitude"]).combine_chunks()
            # Lonboard will automatically map the points
            table = ac.Table.from_arrow(df).append_column("geometry", points([lon_arr, lat_arr]))
            
            # bright white and slightly larger than planes
            cached_layers["celestrak"] = lonboard.ScatterplotLayer(
                table=table, 
                get_fill_color=[255, 255, 255, 255], 
                get_radius=8000, 
                radius_min_pixels=3
            )

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
    
    if switch_satellites.value and "celestrak" in cached_layers:
        active_layers.append(cached_layers["celestrak"])
        total_tracked += cached_counts["celestrak"]
        
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

    # Safely combine the static base layers (cables) with the dynamic active layers!
    interactive_map.layers = base_layers + active_layers

    # Update Data Grids 
    if "google_news_reuters" in data:
        df = pl.from_arrow(data["google_news_reuters"])
        if len(df) > 0:
            news_grid.value = df.to_pandas() 
            
    if "polymarket" in data:
        df = pl.from_arrow(data["polymarket"])
        if len(df) > 0:
            poly_grid.value = df.sort("volume", descending=True).to_pandas()

    # Update Telegram 
    new_messages = []
    for tg_src in list(data.keys()): 
        if tg_src.startswith("tg_"):
            df = pl.from_arrow(data[tg_src])
            if len(df) > 0:
                df = df.sort("date", descending=False)
                for row in df.iter_rows(named=True):
                    new_messages.append(create_tg_card(row['channel'], row['text'], row['date'], row.get('media', '')))
                
    if new_messages:
        feed_container.objects = new_messages + feed_container.objects[:100]

    if total_tracked > 0:
        system_stats.object = f"🟢 **Engine Status:** Online\n📡 **Tracking:** {total_tracked:,} events"

pn.state.add_periodic_callback(update_dashboard, period=5000)


template = pn.template.FastListTemplate(
    title="OSINTxpress Global Monitor",
    theme="dark",
    header_background="#0d1117",
    header=[pn.layout.HSpacer(), log_btn],
    main=[master_tabs],
    main_layout=None 
)

template.modal.append(pn.Column())
template.servable()