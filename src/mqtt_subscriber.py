# -*- coding: utf-8 -*-
"""OpenElectricity — MQTT Subscriber to JSONL

Subscribes to a topic and appends clean JSON lines to a file.
- Only writes when payload is a valid JSON object.
- Mirrors lat/lon to latitude/longitude if needed.
- Keeps the same schema fields used by the publisher.
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from datetime import datetime

import paho.mqtt.client as mqtt


def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def run(args):
    outp = Path(args.out)
    _ensure_dir(outp)
    f = outp.open("a", encoding="utf-8", newline="\n")

    client = mqtt.Client()

    def on_connect(c, userdata, flags, rc):
        print(f"[subscriber] connected rc={rc}, subscribe '{args.topic}' qos={args.qos}")
        c.subscribe(args.topic, qos=args.qos)

    def on_message(c, userdata, msg):
        raw = msg.payload.decode("utf-8", errors="replace")
        now = datetime.now().strftime("%H:%M:%S")
        try:
            obj = json.loads(raw)
        except Exception:
            print(f"[subscriber] non-JSON @ {now}: {raw[:120]}")
            return

        if not isinstance(obj, dict):
            print(f"[subscriber] ignored non-object payload @ {now}")
            return

        # Mirror coords if needed
        if "latitude" not in obj and "lat" in obj:
            obj["latitude"] = obj.get("lat")
        if "longitude" not in obj and "lon" in obj:
            obj["longitude"] = obj.get("lon")

        # Console pulse
        fac = obj.get("facility_name") or obj.get("name") or obj.get("facility_id")
        ts  = obj.get("timestamp")
        pw  = obj.get("power_mw") or obj.get("power")
        print(f"[msg] {fac} @ {ts} | P={pw}")

        # Append one clean JSON line
        line = json.dumps(obj, ensure_ascii=False)
        f.write(line + "\n")
        f.flush()

    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[subscriber] connect {args.broker}:{args.port}")
    client.connect(args.broker, args.port, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[subscriber] stopped by user")
    finally:
        try:
            f.close()
        except Exception:
            pass


def build_parser():
    ap = argparse.ArgumentParser(description="MQTT subscriber -> JSONL")
    ap.add_argument("--broker", required=True)
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    ap.add_argument("--out", type=Path, default=Path("output/sub_received.jsonl"))
    return ap


if __name__ == "__main__":
    run(build_parser().parse_args())

