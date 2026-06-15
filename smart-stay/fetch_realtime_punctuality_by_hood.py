import re
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.cloud import storage, bigquery
from datetime import datetime, timezone

PROJECT = "stay-smart-498122"
BUCKET = "smart-stay-data"
TABLE = f"{PROJECT}.smart_stay.realtime_punctuality_by_hood"

def station_of(stop_id):
    # collapse de:11000:900160004:2:53 -> de:11000:900160004
    m = re.match(r"(de:\d+:\d+)", stop_id)
    return m.group(1) if m else None

def run():
    # Load the stop->neighbourhood lookup, collapse to station level
    client = storage.Client()
    client.bucket(BUCKET).blob("processed/stop_to_neighbourhood.csv").download_to_filename("/tmp/lookup.csv")
    lookup = pd.read_csv("/tmp/lookup.csv")
    lookup["station"] = lookup["stop_id"].apply(station_of)
    # one neighbourhood per station (dedupe)
    station_to_hood = lookup.dropna(subset=["station"]).drop_duplicates("station").set_index("station")["neighbourhood"].to_dict()

    #  Fetch + parse the live feed
    resp = requests.get("https://production.gtfsrt.vbb.de/data", timeout=60)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    # Collect (neighbourhood, delay) for every stop update that has both
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        for stu in entity.trip_update.stop_time_update:
            if not stu.HasField("stop_id"):
                continue
            if not (stu.HasField("arrival") and stu.arrival.HasField("delay")):
                continue
            station = station_of(stu.stop_id)
            hood = station_to_hood.get(station)
            if hood is None:
                continue
            delay = stu.arrival.delay
            if -7200 < delay < 7200:        # drop data quirks
                rows.append({"neighbourhood": hood, "delay": delay})

    df = pd.DataFrame(rows)
    print("Matched delay records:", len(df))
    if df.empty:
        print("No matched delays this snapshot — exiting.")
        return

    # Aggregate per neighbourhood
    snap_time = datetime.now(timezone.utc).isoformat()
    agg = (df.assign(on_time=df["delay"].abs() <= 60)
             .groupby("neighbourhood")
             .agg(updates=("delay", "size"),
                  pct_on_time=("on_time", lambda s: round(100*s.mean(), 1)),
                  avg_delay_seconds=("delay", lambda s: round(s.mean(), 1)))
             .reset_index())
    agg["snapshot_time"] = snap_time

    print(agg.sort_values("updates", ascending=False).head(10).to_string(index=False))

    # Append to BigQuery
    bq = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    bq.load_table_from_dataframe(agg, TABLE, job_config=job_config).result()
    print(f"Appended {len(agg)} neighbourhood rows to {TABLE}")

if __name__ == "__main__":
    run()