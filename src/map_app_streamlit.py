# -*- coding: utf-8 -*-
"""
OpenElectricity — Live Emission Map (Streamlit, CO2 in kg → tonnes)

What this app does:
- Prefer JSONL stream from subscriber; fall back to CSV when JSONL is absent.
- If neither exists (e.g., on Streamlit Cloud), run a background MQTT subscriber thread.
- Convert co2_kg → emissions_tonnes for readable KPIs.
- Deduplicate to the latest record per facility_id (up to a moving time window).
- Keep map view (center/zoom) across reruns so user zoom/pan is not lost.
- Only reload file-data when the file's mtime actually changes.
- Progressive playback (file-based): metrics grow over time instead of jumping to the end.
- Cloud mode (MQTT direct): live updates via st.session_state, auto-refresh every N seconds.
"""

from __future__ import annotations
import os, html, json, threading, time
from pathlib import Path
from typing import Dict, Iterable, Any, Deque
from collections import deque

import pandas as pd
import folium
from folium.plugins import MarkerCluster
import streamlit as st
from streamlit_folium import st_folium


# -------------- Optional (only needed for cloud MQTT) --------------
try:
    import paho.mqtt.client as mqtt  # noqa: F401
    _MQTT_AVAILABLE = True
except Exception:
    _MQTT_AVAILABLE = False

# -------------------------
# Config
# -------------------------
APP_TITLE     = "OpenElectricity — Live Emission Map"
JSONL_PATH    = Path("output/sub_received.jsonl")
CSV_PATH      = Path("data/cleaned_data_mqtt.csv")
DEFAULT_CENTER = (-25.5, 134.5)  # AU-ish
DEFAULT_ZOOM   = 4
PLAYBACK_DELTA = pd.Timedelta(minutes=5)  # file-based playback granularity

# Cloud / MQTT direct config (override via env)
BROKER = os.getenv("BROKER", "test.mosquitto.org")
PORT   = int(os.getenv("PORT", "1883"))
TOPIC  = os.getenv("TOPIC", "openelectricity/power-emissions")
QOS    = int(os.getenv("QOS", "1"))
MQTT_ENABLE_ENV = os.getenv("MQTT_ENABLE", "").strip()  # "1" to force cloud mode

# -------------------------
# Page header
# -------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

# -------------------------
# Color palette
# -------------------------
_PALETTE = [
    "#4C78A8", "#F58518", "#54A24B", "#E45756", "#72B7B2",
    "#EECA3B", "#B279A2", "#FF9DA6", "#9D755D", "#BAB0AC"
]

def make_color_map(fuels: Iterable[str]) -> Dict[str, str]:
    fuels = [f if f else "Unknown" for f in fuels]
    uniq = list(dict.fromkeys(fuels))
    return {f: _PALETTE[i % len(_PALETTE)] for i, f in enumerate(uniq)} | {"Unknown": "#BAB0AC"}

def _valid_lat_lon(lat: Any, lon: Any) -> bool:
    try:
        la = float(lat); lo = float(lon)
    except Exception:
        return False
    if pd.isna(la) or pd.isna(lo): return False
    if not (-90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0): return False
    if abs(la) < 1e-9 and abs(lo) < 1e-9: return False
    return True

def _mtime() -> float:
    if JSONL_PATH.exists(): return JSONL_PATH.stat().st_mtime
    if CSV_PATH.exists():   return CSV_PATH.stat().st_mtime
    return 0.0

def _harmonize(df: pd.DataFrame) -> pd.DataFrame:
    """Unify schema and basic cleaning for file-based data."""
    df = df.copy()

    # coordinates
    if "latitude" not in df.columns and "lat" in df.columns:
        df["latitude"] = df["lat"]
    if "longitude" not in df.columns and "lon" in df.columns:
        df["longitude"] = df["lon"]

    # names
    if "name" not in df.columns and "facility_name" in df.columns:
        df["name"] = df["facility_name"]

    # power
    if "power_mw" not in df.columns and "power" in df.columns:
        df["power_mw"] = df["power"]

    # CO2 mass in kg → tonnes for display
    if "emissions_tonnes" not in df.columns:
        if "co2_kg" in df.columns:
            df["emissions_tonnes"] = pd.to_numeric(df["co2_kg"], errors="coerce") / 1000.0
        elif "emissions" in df.columns:
            df["emissions_tonnes"] = pd.to_numeric(df["emissions"], errors="coerce") / 1000.0
        else:
            df["emissions_tonnes"] = None

    # timestamp → _ts (UTC)
    if "timestamp" not in df.columns and "time" in df.columns:
        df["timestamp"] = df["time"]
    df["_ts"] = pd.to_datetime(df.get("timestamp", pd.Series(dtype=str)), utc=True, errors="coerce")

    # ensure required cols exist
    for c in ["facility_id","name","state","fuel_tech","latitude","longitude","power_mw","timestamp"]:
        if c not in df.columns:
            df[c] = None

    # numerics
    for c in ["latitude","longitude","power_mw","emissions_tonnes"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # sane coords only
    df = df[df.apply(lambda r: _valid_lat_lon(r["latitude"], r["longitude"]), axis=1)]
    return df

def _popup(row: dict) -> str:
    def n(v, default="-"):
        if v is None: return default
        if isinstance(v, float):
            try: return f"{v:,.3f}"
            except: return str(v)
        return html.escape(str(v))
    return (
        "<div style='font-size:14px;line-height:1.45'>"
        f"<b>{n(row.get('name'))}</b><br/>"
        f"<b>ID:</b> {n(row.get('facility_id'))}<br/>"
        f"<b>State:</b> {n(row.get('state'))} | <b>Fuel:</b> {n(row.get('fuel_tech'))}<br/>"
        f"<b>Power (MW):</b> {n(row.get('power_mw'))} | "
        f"<b>CO₂ (tCO₂e):</b> {n(row.get('emissions_tonnes'))}<br/>"
        f"<b>Last Update:</b> {n(row.get('timestamp'))}"
        "</div>"
    )

def _legend(m: folium.Map, cmap: Dict[str, str]) -> None:
    items = "".join(f"<li><span style='background:{c};'></span>{html.escape(k)}</li>" for k,c in cmap.items())
    html_legend = f"""
    <div id="legend" style="position: fixed; bottom: 18px; left: 18px;
         z-index: 9999; background: white; padding: 10px 12px;
         border: 1px solid #ddd; border-radius: 8px;">
      <b style="font-size:13px">Fuel Type</b>
      <ul style="margin:6px 0 0 0; padding:0; list-style:none; font-size:12px">{items}</ul>
    </div>
    <style>
      #legend ul li {{ display:flex; align-items:center; gap:8px; margin:3px 0; }}
      #legend ul li span {{ width:12px; height:12px; border-radius:50%; display:inline-block; border:1px solid rgba(0,0,0,.25); }}
    </style>
    """
    m.get_root().html.add_child(folium.Element(html_legend))

def _markers(m: folium.Map, df: pd.DataFrame, cmap: Dict[str, str]) -> None:
    cluster = MarkerCluster(disableClusteringAtZoom=9).add_to(m)
    for _, r in df.iterrows():
        col = cmap.get(r.get("fuel_tech","Unknown"), "#BAB0AC")
        pw  = float(r.get("power_mw") or 0.0)
        radius = max(4, min(12, (pw if pw > 0 else 4) ** 0.5 + 4))
        folium.CircleMarker(
            location=(float(r["latitude"]), float(r["longitude"])),
            radius=radius, color="#2b2b2b", weight=0.8,
            fill=True, fill_color=col, fill_opacity=0.88,
            tooltip=f"{r.get('name','Unknown')} | {r.get('fuel_tech','')}",
            popup=folium.Popup(_popup(r), max_width=360),
        ).add_to(cluster)
    folium.LayerControl(collapsed=False).add_to(m)

# -------------------------
# Session state (shared)
# -------------------------
if "data_mtime" not in st.session_state:
    st.session_state.data_mtime = -1.0
if "df_all" not in st.session_state:
    st.session_state.df_all = pd.DataFrame()
if "map_view" not in st.session_state:
    st.session_state.map_view = {"center": list(DEFAULT_CENTER), "zoom": DEFAULT_ZOOM}
if "show_upto_ts" not in st.session_state:
    st.session_state.show_upto_ts = None

# MQTT session buffers (cloud mode)
if "mqtt_started" not in st.session_state:
    st.session_state.mqtt_started = False
if "mqtt_status" not in st.session_state:
    st.session_state.mqtt_status = "idle"
if "msg_buf" not in st.session_state:
    st.session_state.msg_buf = deque(maxlen=20000)
if "latest_by_fac" not in st.session_state:
    st.session_state.latest_by_fac = {} 

# -------------------------
# Sidebar controls
# -------------------------
with st.sidebar:
    st.subheader("Control Panel")
    # Decide cloud mode:
    cloud_force = st.checkbox(
        "Cloud mode (subscribe MQTT directly)",
        value=(MQTT_ENABLE_ENV == "1")
    )
    pause_update = st.checkbox("Pause Data Update (file-based)", value=False, key="pause_update")
    play = st.checkbox("Progressive reveal (file-based)", value=True, key="play_flag")
    speed = st.slider("Playback speed (sec per tick)", 0.2, 3.0, 1.0, 0.1, key="play_speed")
    refresh_every = st.slider("Auto-refresh (cloud mode, seconds)", 1, 10, 2, 1, key="refresh_interval")

    if st.button("Manual Refresh (file-based)", key="btn_manual_refresh"):
        st.session_state.data_mtime = -1.0
        st.rerun()

# ---------- Decide data mode ----------
has_jsonl = JSONL_PATH.exists()
has_csv   = CSV_PATH.exists()
cloud_mode_auto = (not has_jsonl and not has_csv) or (MQTT_ENABLE_ENV == "1")
CLOUD_MODE = cloud_force or cloud_mode_auto

# -------------------------
# CLOUD MODE: MQTT background subscriber
# -------------------------
def _to_df_from_latest(latest: dict[str, dict]) -> pd.DataFrame:
    if not latest:
        return pd.DataFrame(columns=[
            "facility_id","facility_code","name","facility_name","state","region",
            "fuel_tech","latitude","longitude","power_mw","co2_kg","emissions_tonnes","timestamp","_ts"
        ])
    df = pd.DataFrame(latest.values()).copy()

    # schema harmonisation for live records
    if "facility_id" not in df.columns:
        df["facility_id"] = df.get("facility_code") or df.get("code")
    if "name" not in df.columns:
        df["name"] = df.get("facility_name")
    if "power_mw" not in df.columns and "power" in df.columns:
        df["power_mw"] = df["power"]
    if "co2_kg" not in df.columns and "emissions" in df.columns:
        df["co2_kg"] = df["emissions"]

    # emissions tonnes
    df["emissions_tonnes"] = pd.to_numeric(df.get("co2_kg"), errors="coerce") / 1000.0

    # timestamp
    ts_col = "timestamp" if "timestamp" in df.columns else ("ts" if "ts" in df.columns else None)
    if ts_col:
        df["_ts"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        df["timestamp"] = df[ts_col]
    else:
        df["_ts"] = pd.NaT

    # numerics & coords
    for c in ("latitude","longitude","power_mw"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "latitude" in df and "longitude" in df:
        df = df[df.apply(lambda r: _valid_lat_lon(r["latitude"], r["longitude"]), axis=1)]

    # fill essentials
    for c in ["facility_id","name","state","fuel_tech","latitude","longitude","power_mw","timestamp"]:
        if c not in df.columns: df[c] = None

    return df

def _mqtt_on_connect(client, userdata, flags, rc):
    st.session_state.mqtt_status = f"connected rc={rc}"
    client.subscribe(TOPIC, qos=QOS)

def _mqtt_on_message(client, userdata, msg):
    try:
        j = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except Exception:
        return
    # normalise keys
    fc = j.get("facility_code") or j.get("facility_id") or j.get("code")
    if not fc:
        return
    rec = {
        "facility_code": fc,
        "facility_id": j.get("facility_id") or fc,
        "facility_name": j.get("facility_name") or j.get("name"),
        "name": j.get("facility_name") or j.get("name"),
        "region": (j.get("region") or j.get("network_region") or "").strip().upper() or None,
        "state": (j.get("state") or j.get("region_state") or None),
        "fuel_tech": j.get("fuel_tech") or j.get("fueltech_id") or "unknown",
        "latitude": j.get("latitude") or j.get("lat"),
        "longitude": j.get("longitude") or j.get("lon") or j.get("lng"),
        "power_mw": j.get("power_mw") if "power_mw" in j else j.get("power"),
        "co2_kg": j.get("co2_kg") if "co2_kg" in j else (j.get("emissions") or j.get("co2")),
        "timestamp": j.get("timestamp") or j.get("ts"),
    }
    st.session_state.msg_buf.append(rec)
    st.session_state.latest_by_fac[fc] = rec

def _start_mqtt_once():
    if st.session_state.mqtt_started or not _MQTT_AVAILABLE:
        return
    st.session_state.mqtt_started = True
    st.session_state.mqtt_status = "connecting…"

    def _run():
        try:
            client = mqtt.Client()  # v1 API works cross-env
            client.on_connect = _mqtt_on_connect
            client.on_message = _mqtt_on_message
            client.connect(BROKER, PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            st.session_state.mqtt_status = f"MQTT error: {e}"

    th = threading.Thread(target=_run, daemon=True)
    th.start()

# -------------------------
# Data source selection
# -------------------------
if CLOUD_MODE and _MQTT_AVAILABLE:
    # Cloud / live mode
    _start_mqtt_once()
    df_all = _to_df_from_latest(st.session_state.latest_by_fac)
    file_based = False
    # In live mode, playback doesn't make sense
    play = False
else:
    # File-based mode (JSONL preferred, else CSV)
    mt = _mtime()
    if not st.session_state.get("pause_update", False) and mt != st.session_state.data_mtime:
        if JSONL_PATH.exists():
            raw = pd.read_json(JSONL_PATH, lines=True)
        elif CSV_PATH.exists():
            raw = pd.read_csv(CSV_PATH)
        else:
            raw = pd.DataFrame()
        st.session_state.df_all = _harmonize(raw) if not raw.empty else pd.DataFrame()
        st.session_state.data_mtime = mt
    df_all = st.session_state.df_all
    file_based = True

# -------------------------
# If no data yet → draw an empty basemap so the page is never blank
# -------------------------
if df_all.empty:
    with st.sidebar:
        if CLOUD_MODE:
            st.write("MQTT:", st.session_state.get("mqtt_status", "starting…"))
            st.caption(f"Broker: {BROKER}, Topic: {TOPIC}")
    st.info("Waiting for data… Start publisher, or enable cloud mode to subscribe directly.")
    m = folium.Map(location=DEFAULT_CENTER, zoom_start=DEFAULT_ZOOM, tiles="cartodbpositron")
    st_folium(m, height=640, use_container_width=True, key="folium_empty")
    # In live mode, auto refresh to pull arriving messages
    if CLOUD_MODE:
        st.autorefresh(interval=int(st.session_state.get("refresh_interval", 2)) * 1000, key="poll-empty")
    st.stop()

# -------------------------
# Progressive time window (file-based only)
# -------------------------
if file_based:
    min_ts = pd.to_datetime(df_all["_ts"]).min()
    max_ts = pd.to_datetime(df_all["_ts"]).max()

    if st.session_state.show_upto_ts is None or pd.isna(st.session_state.show_upto_ts):
        st.session_state.show_upto_ts = min_ts

    if play and st.session_state.show_upto_ts < max_ts:
        st.session_state.show_upto_ts = min(
            max_ts,
            pd.to_datetime(st.session_state.show_upto_ts) + PLAYBACK_DELTA
        )

    df_window = df_all[df_all["_ts"] <= st.session_state.show_upto_ts].copy()
    if not df_window.empty:
        df_latest = df_window.sort_values("_ts").groupby("facility_id", as_index=False).tail(1)
    else:
        df_latest = df_window
else:
    # live mode: latest_by_fac is already "latest"
    df_latest = df_all.copy()

# -------------------------
# KPIs
# -------------------------
def _fmt(x):
    try: return f"{x:,.1f}"
    except: return "-"

latest_ts = pd.to_datetime(df_latest["_ts"]).max() if "_ts" in df_latest.columns and not df_latest.empty else None
c1, c2, c3 = st.columns(3)
c1.metric("Facilities", f"{df_latest['facility_id'].nunique() if not df_latest.empty else 0}")
c2.metric("Total Power (MW)", _fmt(df_latest["power_mw"].fillna(0).sum() if not df_latest.empty else 0))
c3.metric("Total CO₂ (tCO₂e)", _fmt(df_latest["emissions_tonnes"].fillna(0).sum() if "emissions_tonnes" in df_latest.columns and not df_latest.empty else 0))
st.caption(f"Last update: {latest_ts if latest_ts is not None else '-'}")

# -------------------------
# Filter (single multiselect, unique key)
# -------------------------
fuels = sorted([x for x in df_all["fuel_tech"].dropna().unique().tolist() if x]) if "fuel_tech" in df_all.columns else []
selected = st.multiselect("Filter by fuel type", options=fuels, default=fuels, key="fuel_select_1")
plot_df = df_latest[df_latest["fuel_tech"].isin(selected)] if selected else (df_latest if fuels else df_latest.iloc[0:0])

# -------------------------
# Map
# -------------------------
center = tuple(st.session_state.map_view.get("center", list(DEFAULT_CENTER)))
zoom   = int(st.session_state.map_view.get("zoom", DEFAULT_ZOOM))
m = folium.Map(location=center, zoom_start=zoom, tiles="cartodbpositron")
cmap = make_color_map(plot_df["fuel_tech"].tolist() if "fuel_tech" in plot_df.columns else ["Unknown"])
_markers(m, plot_df, cmap)
_legend(m, cmap)

ret = st_folium(m, height=680, use_container_width=True, key="folium_map_v1")
if isinstance(ret, dict):
    new_center = ret.get("center") or ret.get("last_center")
    new_zoom   = ret.get("zoom")
    if new_center or new_zoom is not None:
        st.session_state.map_view = {
            "center": [float(new_center["lat"]), float(new_center["lng"])] if isinstance(new_center, dict) else list(center),
            "zoom": int(new_zoom) if new_zoom is not None else zoom,
        }

# -------------------------
# Tick & rerun
# -------------------------
if file_based:
    # Sleep = UI tick duration; keeps user interactions usable while the head advances.
    time.sleep(float(st.session_state.get("play_speed", 1.0)))
    if st.session_state.get("play_flag", False) and not df_all.empty:
        # If still playing and not yet at the end, rerun to animate forward
        max_ts_all = pd.to_datetime(df_all["_ts"]).max()
        if st.session_state.show_upto_ts < max_ts_all:
            st.rerun()
else:
    # Cloud/live mode: periodic auto-refresh pulls newly arrived messages from background MQTT
    st.autorefresh(interval=int(st.session_state.get("refresh_interval", 2)) * 1000, key="poll-live")
