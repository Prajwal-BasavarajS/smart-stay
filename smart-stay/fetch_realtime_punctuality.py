import requests
from google.transit import gtfs_realtime_pb2
from google.cloud import bigquery
from datetime import datetime, timezone

RT_URL = "https://production.gtfsrt.vbb.de/data"
PROJECT = "stay-smart-498122"
TABLE = f"{PROJECT}.smart_stay.realtime_punctuality"

def fetch_punctuality():
    resp = requests.get(RT_URL, timeout=60)
    resp.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    delays = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            for stu in entity.trip_update.stop_time_update:
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    d = stu.arrival.delay
                    # filter out obvious data quirks (>2h is noise)
                    if -7200 < d < 7200:
                        delays.append(d)

    total = len(delays)
    on_time = sum(1 for d in delays if abs(d) <= 60)
    late    = sum(1 for d in delays if d > 60)
    early   = sum(1 for d in delays if d < -60)
    avg_delay = round(sum(delays) / total, 1) if total else 0

    snapshot = {
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "feed_timestamp": int(feed.header.timestamp),
        "active_trips": len(feed.entity),
        "total_stop_updates": total,
        "on_time": on_time,
        "late": late,
        "early": early,
        "pct_on_time": round(100 * on_time / total, 1) if total else 0,
        "avg_delay_seconds": avg_delay,
    }
    print("Punctuality snapshot:", snapshot)

    # Write to BigQuery (append — builds a time series over runs)
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("snapshot_time", "TIMESTAMP"),
            bigquery.SchemaField("feed_timestamp", "INTEGER"),
            bigquery.SchemaField("active_trips", "INTEGER"),
            bigquery.SchemaField("total_stop_updates", "INTEGER"),
            bigquery.SchemaField("on_time", "INTEGER"),
            bigquery.SchemaField("late", "INTEGER"),
            bigquery.SchemaField("early", "INTEGER"),
            bigquery.SchemaField("pct_on_time", "FLOAT"),
            bigquery.SchemaField("avg_delay_seconds", "FLOAT"),
        ],
    )
    client.load_table_from_json([snapshot], TABLE, job_config=job_config).result()
    print(f"Appended snapshot to {TABLE}")

if __name__ == "__main__":
    fetch_punctuality()