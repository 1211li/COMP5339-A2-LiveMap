# COMP5339 Assignment 2 — Real-time Electricity Emission Streaming

### Group: TUT08-ASSIGNMENTGRP-03  
**Members:**  
- **Hsin-I Chiu (550347961)** – Data Retrieval & Cleaning  
- **Jianwei Li (530228127)** – MQTT Streaming & Visualisation  
- **Junzhi Chen (530696854)** – System Integration & Testing  

---

## 1. Project Overview
This project develops a small end-to-end system that streams and visualises real-time electricity generation and CO₂-emission data from the Australian National Electricity Market (NEM).  
Using the **OpenElectricity API** as the main data source, we built a Python-based pipeline that automatically collects, cleans, publishes, subscribes, and displays live data on a dynamic dashboard.

The goal was to demonstrate how open energy data can be transformed into an interactive real-time application using modern data-engineering tools such as **MQTT**, **Pandas**, and **Streamlit**.

---

## 2. System Components

### (1) Data Retrieval – Task 1  
Implemented by Hsin-I Chiu using the OpenElectricity REST API.  
- The `/v4/facilities/` endpoint provides metadata (name, region, coordinates, fuel type).  
- The `/v4/facility_timeseries/` endpoint returns one week of 5-minute interval records for power and emissions.  
- The script handles pagination, batching, and HTTP 416 retries.  

Cleaned outputs:  
```
data/facilities.csv  
data/facility_timeseries_all.csv
```

---

### (2) Data Cleaning & Integration – Task 2  
The two datasets were merged and standardised. We fixed region codes, removed duplicates, and joined on `facility_code`.  
Negative power values for non-battery plants were set to zero.   
The final CSV used for streaming is:  
```
data/cleaned_data_mqtt.csv
```

---

### (3) MQTT Publishing – Task 3  
Implemented by Jianwei Li in `src/mqtt_publisher_loop.py`.  
The publisher reads `cleaned_data_mqtt.csv` and streams each record as JSON to the public broker **test.mosquitto.org**.  
Each message includes:  
```
facility_id, facility_name, region, fuel_tech, latitude, longitude, power_mw, co2_kg, timestamp
```
Main design choices:  
- 0.1-second delay between messages (simulated real-time flow)  
- 60-second pause between full replays  
- Only new or changed records re-published to reduce load  

---

### (4) MQTT Subscription & Live Visualisation – Task 4  
Implemented in `src/mqtt_subscriber.py` and `src/map_app_streamlit.py`.  
- The subscriber listens to the same topic and stores each message as JSON lines in:  
  ```
  output/sub_received.jsonl
  ```  
- The Streamlit dashboard renders these updates on an interactive Folium map in real time.

Dashboard features include:  
- Dynamic marker updates without resetting zoom or center  
- Facility pop-ups with power (MW) and emissions (t CO₂e)  
- Sidebar filters by fuel type (and optional region)  
- Optional display of network price and demand  

---

## 3. How to Run

### Step 1 — Set up environment
```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 2 — Start Subscriber
```bash
python src/mqtt_subscriber.py   --broker test.mosquitto.org --port 1883   --topic openelectricity/power-emissions --qos 1   --out output/sub_received.jsonl
```

### Step 3 — Start Publisher
```bash
python src/mqtt_publisher_loop.py   --csv data/cleaned_data_mqtt.csv   --broker test.mosquitto.org --port 1883   --topic openelectricity/power-emissions --qos 1   --rate-delay 0.1 --sleep 60
```

### Step 4 — Launch Dashboard
```bash
streamlit run src/map_app_streamlit.py
```
When Streamlit starts, open the printed link (e.g. [http://localhost:8501](http://localhost:8501)) to see the live map updating with facility data.

---

## 4. Optimisation and Efficiency
During testing, we added several optimisations:
- Cached API results and CSV materialisation to avoid redundant calls.  
- Incremental publishing based on `last_sent_state.json`.  
- Streamlit state preservation so map does not re-render each refresh.  
- Lightweight JSONL tail reading to keep updates fast.  

These changes helped the system stay responsive and stable over long runs.

---

## 5. Known Limitations
- The public broker may occasionally disconnect under heavy traffic.  
- Streamlit updates every few seconds rather than true real-time.  
- Some facilities lack precise coordinates or fuel type metadata.  

---

## 6. Reflection
This project gave us hands-on experience with real-time data engineering.  
We learned how to design a streaming workflow using open APIs, MQTT messaging, and interactive visualisation.  
Although the prototype is simple, it captures the core idea of continuous data integration and real-time insight generation.  
It also laid the foundation for future work on scalable energy dashboards.

---

## Submission Files
```
├── data/
│   ├── facilities.csv
│   ├── facility_timeseries_all.csv
│   ├── cleaned_data_mqtt.csv
├── output/
│   └── sub_received.jsonl
├── src/
│   ├── mqtt_publisher_loop.py
│   ├── mqtt_subscriber.py
│   ├── map_app_streamlit.py
├── run_all.sh        # Mac/Linux
├── run_all.bat       # Windows
├── requirements.txt
└── README_final.md
```

---

© University of Sydney | COMP5339 S2 2025 – Group W02_G08