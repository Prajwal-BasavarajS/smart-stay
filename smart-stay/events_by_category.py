# events per neighbourhood + category
import geopandas as gpd
import pandas as pd
from google.cloud import storage, bigquery

PROJECT = "stay-smart-498122"
BUCKET = "smart-stay-data"
TABLE = f"{PROJECT}.smart_stay.events_by_category"

def run():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    bucket.blob("raw/berlin_events.csv").download_to_filename("/tmp/berlin_events.csv")
    bucket.blob("berlin_neighbourhoods.geojson").download_to_filename("/tmp/hoods.geojson")

    events = pd.read_csv("/tmp/berlin_events.csv")
    hoods = gpd.read_file("/tmp/hoods.geojson")

    events = events.dropna(subset=["latitude", "longitude"])
    events["category"] = events["category"].fillna("Other")
    print("Events with coordinates:", len(events))

    events_gdf = gpd.GeoDataFrame(
        events,
        geometry=gpd.points_from_xy(events["longitude"], events["latitude"]),
        crs="EPSG:4326",
    )
    hoods = hoods.to_crs("EPSG:4326")

    joined = gpd.sjoin(events_gdf, hoods, how="left", predicate="within")
    matched = joined.dropna(subset=["neighbourhood"])

    # per neighbourhood + category (raw counts)
    by_cat = (matched
              .groupby(["neighbourhood", "category"])
              .size()
              .reset_index(name="event_count"))

    # total events per neighbourhood, merged on for convenience
    totals = (matched
              .groupby("neighbourhood")
              .size()
              .reset_index(name="total_events"))

    out = by_cat.merge(totals, on="neighbourhood", how="left")

    print(f"Rows (neighbourhood x category): {len(out)}")
    print(out.sort_values("event_count", ascending=False).head(10).to_string(index=False))

    bq = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(out, TABLE, job_config=job_config).result()
    print(f"Wrote {TABLE} ({len(out)} rows)")

if __name__ == "__main__":
    run()