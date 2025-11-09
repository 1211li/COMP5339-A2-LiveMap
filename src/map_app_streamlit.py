# -*- coding: utf-8 -*-
"""
OpenElectricity — Live Emission Map (Streamlit, CO2 in kg → tonnes)

What this app does:
- Prefer JSONL stream from subscriber; fall back to CSV when JSONL is absent.
- Convert co2_kg → emissions_tonnes for readable KPIs.
- Deduplicate to the latest record per facility_id (up to a moving time window).
- Keep map view (center/zoom) across reruns so user zoom/pan is not lost.
- Only reload data when the file's mtime actually changes.
- Progressive playback: metrics grow over time instead of jumping to the end.
"""

from __future__ import annotations
import html
from pathlib import Path
from typing import Dict, Iterable, Any
import time

import pandas as pd
import folium
from folium.plugins import MarkerCluster
import streamlit as st
from streamlit_folium import st_folium

# -------------------------
# Config
# -------------------------
APP_TITLE   = "OpenElectricity — Live Emission Map"
JSONL_PATH  = Path("output/sub_received.jsonl")
CSV_PATH    = Path("data/cleaned_data_mqtt.csv")
DEFAULT_CENTER = (-25.5, 134.5)  # AU-ish
DEFAULT_ZOOM   = 4

# Data playback granularity (how fast the business time moves per tick)
PLAYBACK_DELTA = pd.Timedelta(minutes=5)

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
    """Unify schema and basic cleaning."""
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
# Session state
# -------------------------
if "data_mtime" not in st.session_state:
    st.session_state.data_mtime = -1.0
if "df_all" not in st.session_state:
    st.session_state.df_all = pd.DataFrame()
if "map_view" not in st.session_state:
    st.session_state.map_view = {"center": list(DEFAULT_CENTER), "zoom": DEFAULT_ZOOM}
if "show_upto_ts" not in st.session_state:
    st.session_state.show_upto_ts = None

# -------------------------
# Sidebar controls
# -------------------------
with st.sidebar:
    st.subheader("Control Panel")
    st.caption("Reload only when data file mtime changes.")
    play = st.checkbox("Progressive reveal (playback)", value=True, key="play_flag")
    speed = st.slider("Playback speed (sec per tick)", 0.2, 3.0, 1.0, 0.1, key="play_speed")
    pause_update = st.checkbox("Pause Data Update", value=False, key="pause_update")
    refresh_every = st.slider("Data Refresh Interval (s)", 0.5, 5.0, 1.5, 0.1, key="refresh_interval")

    if st.button("Manual Refresh", key="btn_manual_refresh"):
        st.session_state.data_mtime = -1.0
        st.rerun()

# -------------------------
# Data loading (only when mtime changed and not paused)
# -------------------------
mt = _mtime()
if not pause_update and mt != st.session_state.data_mtime:
    if JSONL_PATH.exists():
        raw = pd.read_json(JSONL_PATH, lines=True)
    elif CSV_PATH.exists():
        raw = pd.read_csv(CSV_PATH)
    else:
        raw = pd.DataFrame()

    st.session_state.df_all = _harmonize(raw) if not raw.empty else pd.DataFrame()
    st.session_state.data_mtime = mt

df_all = st.session_state.df_all

# -------------------------
# If no data yet → draw an empty basemap so the page is never blank
# -------------------------
if df_all.empty:
    st.info("Waiting for data… Start publisher & subscriber.")
    m = folium.Map(location=DEFAULT_CENTER, zoom_start=DEFAULT_ZOOM, tiles="cartodbpositron")
    st_folium(m, height=640, use_container_width=True, key="folium_empty")
    st.stop()

# -------------------------
# Progressive time window
# -------------------------
min_ts = pd.to_datetime(df_all["_ts"]).min()
max_ts = pd.to_datetime(df_all["_ts"]).max()

# initialize window start at the earliest timestamp
if st.session_state.show_upto_ts is None or pd.isna(st.session_state.show_upto_ts):
    st.session_state.show_upto_ts = min_ts

# advance the window head when playing
if play and st.session_state.show_upto_ts < max_ts:
    st.session_state.show_upto_ts = min(
        max_ts,
        pd.to_datetime(st.session_state.show_upto_ts) + PLAYBACK_DELTA
    )

# filter data up to the current head, then keep latest per facility
df_window = df_all[df_all["_ts"] <= st.session_state.show_upto_ts].copy()
if not df_window.empty:
    df_latest = df_window.sort_values("_ts").groupby("facility_id", as_index=False).tail(1)
else:
    # very beginning: show nothing, but keep structure
    df_latest = df_window

# -------------------------
# KPIs
# -------------------------
def _fmt(x):
    try: return f"{x:,.1f}"
    except: return "-"

latest_ts = pd.to_datetime(df_window["_ts"]).max() if not df_window.empty else None
c1, c2, c3 = st.columns(3)
c1.metric("Facilities", f"{df_latest['facility_id'].nunique() if not df_latest.empty else 0}")
c2.metric("Total Power (MW)", _fmt(df_latest["power_mw"].fillna(0).sum() if not df_latest.empty else 0))
c3.metric("Total CO₂ (tCO₂e)", _fmt(df_latest["emissions_tonnes"].fillna(0).sum() if not df_latest.empty else 0))
st.caption(f"Last update: {latest_ts if latest_ts is not None else '-'}")

# -------------------------
# Filter (single multiselect, unique key)
# -------------------------
fuels = sorted([x for x in df_all["fuel_tech"].dropna().unique().tolist() if x])
selected = st.multiselect("Filter by fuel type", options=fuels, default=fuels, key="fuel_select_1")
plot_df = df_latest[df_latest["fuel_tech"].isin(selected)] if selected else df_latest.iloc[0:0]

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
# Sleep = UI tick duration; keeps user interactions usable while the head advances.
time.sleep(float(speed))
# If still playing and not yet at the end, rerun to animate forward
if play and st.session_state.show_upto_ts < max_ts:
    st.rerun()
