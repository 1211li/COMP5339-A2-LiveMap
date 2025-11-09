#!/bin/bash
# run_all.sh — macOS/Linux safe version: background subscriber+publisher, foreground Streamlit

set -euo pipefail

# Go to project root (the folder containing this script)
cd "$(dirname "$0")"
mkdir -p output

echo "[run_all] starting subscriber (background)…"
python3 src/mqtt_subscriber.py \
  --broker test.mosquitto.org --port 1883 \
  --topic openelectricity/power-emissions --qos 1 \
  --out output/sub_received.jsonl \
  > output/subscriber.log 2>&1 & SUB_PID=$!

sleep 2

echo "[run_all] starting publisher (background)…"
python3 src/mqtt_publisher_loop.py \
  --csv data/cleaned_data_mqtt.csv \
  --broker test.mosquitto.org --port 1883 \
  --topic openelectricity/power-emissions --qos 1 \
  --rate-delay 0.1 --sleep 60 \
  > output/publisher.log 2>&1 & PUB_PID=$!

# Ensure background processes get cleaned up when we exit
cleanup() {
  echo "[run_all] stopping background processes…"
  kill "$SUB_PID" "$PUB_PID" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "[run_all] launching Streamlit (foreground)…"
streamlit run src/map_app_streamlit.py
