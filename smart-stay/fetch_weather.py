import requests
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT = "stay-smart-498122"
TABLE = f"{PROJECT}.smart_stay.berlin_conditions"

LAT, LON = 52.52, 13.405

def fetch_weather():
    w = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LAT, "longitude": LON,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,weather_code",
        },
        timeout=10,
    ).json()["current"]

    aq = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": LAT, "longitude": LON,
            "current": "european_aqi,pm2_5,pm10,ozone",
        },
        timeout=10,
    ).json()["current"]

    record = {
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "temperature_c": w["temperature_2m"],
        "humidity_pct": w["relative_humidity_2m"],
        "wind_kmh": w["wind_speed_10m"],
        "rain_mm": w["rain"],
        "weather_code": w["weather_code"],
        "european_aqi": aq["european_aqi"],
        "pm2_5": aq["pm2_5"],
        "pm10": aq["pm10"],
        "ozone": aq["ozone"],
    }
    print(record)

    bq = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    bq.load_table_from_json([record], TABLE, job_config=job_config).result()
    print(f"Appended conditions to {TABLE}")

if __name__ == "__main__":
    fetch_weather()