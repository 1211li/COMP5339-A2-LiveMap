@echo off
REM run_all.bat — start subscriber, publisher and Streamlit app (Windows)

setlocal
cd /d %~dp0

if not exist output mkdir output

echo Starting subscriber...
start cmd /k "python src\mqtt_subscriber.py --broker test.mosquitto.org --port 1883 --topic openelectricity\power-emissions --qos 1 --out output\sub_received.jsonl"

timeout /t 2 >nul

echo Starting publisher...
start cmd /k "python src\mqtt_publisher_loop.py --csv data\cleaned_data_mqtt.csv --broker test.mosquitto.org --port 1883 --topic openelectricity\power-emissions --qos 1 --rate-delay 0.1 --sleep 60"

timeout /t 3 >nul

echo Launching Streamlit...
streamlit run src\map_app_streamlit.py

pause
