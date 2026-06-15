import geopandas as gpd
import pandas as pd
from google.cloud import storage, bigquery

PROJECT = "stay-smart-498122"
BUCKET = "smart-stay-data"
TABLE = f"{PROJECT}.smart_stay.events_score"

def score_events():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    bucket.blob("raw/berlin_events.csv").download_to_filename("/tmp/berlin_events.csv")
    bucket.blob("berlin_neighbourhoods.geojson").download_to_filename("/tmp/hoods.geojson")

    events = pd.read_csv("/tmp/berlin_events.csv")
    hoods = gpd.read_file("/tmp/hoods.geojson")

    events = events.dropna(subset=["latitude", "longitude"])
    print("Events with coordinates:", len(events))

    events_gdf = gpd.GeoDataFrame(
        events,
        geometry=gpd.points_from_xy(events["longitude"], events["latitude"]),
        crs="EPSG:4326",
    )
    hoods = hoods.to_crs("EPSG:4326")

    joined = gpd.sjoin(events_gdf, hoods, how="left", predicate="within")
    print("Events outside all neighbourhoods:", joined["neighbourhood"].isna().sum())

    counts = (joined.dropna(subset=["neighbourhood"])
              .groupby("neighbourhood").size()
              .reset_index(name="event_count"))
    mn, mx = counts["event_count"].min(), counts["event_count"].max()
    counts["events_score"] = ((counts["event_count"] - mn) / (mx - mn) * 100).round(1)

    print(f"Scored {len(counts)} neighbourhoods with events")

    bq = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(counts, TABLE, job_config=job_config).result()
    print(f"Wrote events_score to {TABLE} ({len(counts)} rows)")

if __name__ == "__main__":
    score_events()