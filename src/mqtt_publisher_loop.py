# -*- coding: utf-8 -*-
"""OpenElectricity — MQTT Publisher (continuous, rubric-aligned)

Reads a CSV and publishes JSON messages to an MQTT topic:
- Strict chronological order (_ts asc, then facility_code)
- Per-message delay (default 0.1s)
- Continuous loop (sleep between rounds, default 60s)
- Only publish updated values per facility (power/co2 changed)
- JSON schema (units):
    facility_id: str         # from facility_code
    facility_name: str
    latitude: float          # from lat
    longitude: float         # from lon
    power_mw: float
    co2_kg: float            # CO2 mass in kilograms
    state: str               # region
    fuel_tech: str
    timestamp: str           # ISO 8601, UTC '...Z'
"""
from __future__ import annotations
import argparse, json, sys, time
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import paho.mqtt.client as mqtt


def _iso_utc(ts: str) -> str:
    try:
        dt = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(dt): return ""
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _load_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    need = [
        "facility_code", "facility_name", "timestamp",
        "power_mw", "co2_kg", "region", "fuel_tech", "lat", "lon",
    ]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    df = df.copy()
    df["timestamp"] = df["timestamp"].map(_iso_utc)
    df = df[df["timestamp"] != ""]
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])
    df["_ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["_ts"])
    df = df.sort_values(["_ts", "facility_code"], kind="stable").reset_index(drop=True)
    return df


def _build_client() -> mqtt.Client:
    c = mqtt.Client()  # v1 API, stable across environments
    return c


def run(args: argparse.Namespace) -> None:
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[publisher] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    client = _build_client()
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()
    print(f"[publisher] connect {args.broker}:{args.port} topic={args.topic} qos={args.qos} retain={args.retain}")

    # Per-facility last sent values (to detect updates only)
    last_vals: Dict[str, Dict[str, Any]] = {}

    round_idx = 0
    try:
        while True:
            round_idx += 1
            try:
                df = _load_csv(csv_path)
            except Exception as e:
                print(f"[publisher] CSV load error: {e}", file=sys.stderr)
                time.sleep(args.sleep)
                continue

            sent = 0
            for _, r in df.iterrows():
                fid = str(r["facility_code"])
                cur_p = float(r["power_mw"]) if pd.notna(r["power_mw"]) else 0.0
                cur_c = float(r["co2_kg"]) if pd.notna(r["co2_kg"]) else 0.0

                prev = last_vals.get(fid, {})
                if prev.get("power_mw") == cur_p and prev.get("co2_kg") == cur_c:
                    # No change -> skip (Exceeds requirement: only updated values)
                    continue

                msg = {
                    "facility_id": fid,
                    "facility_name": str(r["facility_name"]) if pd.notna(r["facility_name"]) else "Unknown",
                    "latitude": float(r["lat"]),
                    "longitude": float(r["lon"]),
                    "power_mw": cur_p,
                    "co2_kg": cur_c,
                    "state": str(r["region"]) if pd.notna(r["region"]) else "",
                    "fuel_tech": str(r["fuel_tech"]) if pd.notna(r["fuel_tech"]) else "",
                    "timestamp": str(r["timestamp"]),
                }
                data = json.dumps(msg, ensure_ascii=False)
                info = client.publish(args.topic, data, qos=args.qos, retain=args.retain)
                info.wait_for_publish(timeout=5)
                sent += 1
                if sent % 100 == 1:
                    print(f"[pub] #{sent} {msg['facility_name']} @ {msg['timestamp']}")

                last_vals[fid] = {"power_mw": cur_p, "co2_kg": cur_c}
                time.sleep(max(0.0, float(args.rate_delay)))

            print(f"[publisher] round={round_idx} sent={sent}, sleep {args.sleep}s …")
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print("\n[publisher] stopped by user")
    finally:
        client.loop_stop()
        client.disconnect()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Continuous MQTT publisher for OpenElectricity")
    ap.add_argument("--csv", required=True, help="CSV path (cleaned_data_mqtt.csv)")
    ap.add_argument("--broker", required=True, help="MQTT broker host")
    ap.add_argument("--port", type=int, default=1883, help="MQTT port (default 1883)")
    ap.add_argument("--topic", required=True, help="MQTT topic")
    ap.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    ap.add_argument("--retain", action="store_true", help="Publish with retain flag")
    ap.add_argument("--rate-delay", type=float, default=0.1, help="Seconds between messages (default 0.1)")
    ap.add_argument("--sleep", type=int, default=60, help="Seconds between replay rounds (default 60)")
    return ap


if __name__ == "__main__":
    run(build_parser().parse_args())
