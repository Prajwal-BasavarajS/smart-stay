import geopandas as gpd
from google.cloud import bigquery, storage

PROJECT = "stay-smart-498122"
BUCKET = "smart-stay-data"
GEOJSON_BLOB = "berlin_neighbourhoods.geojson"
TABLE = f"{PROJECT}.smart_stay.neighbourhood_geo"

def run():
    # Download the geojson from GCS
    storage.Client().bucket(BUCKET).blob(GEOJSON_BLOB).download_to_filename("/tmp/hoods.geojson")
    hoods = gpd.read_file("/tmp/hoods.geojson")
    print("Neighbourhoods loaded:", len(hoods))
    print("Columns:", list(hoods.columns))

    # Ensure WGS84 (lat/lon) for BigQuery GEOGRAPHY
    hoods = hoods.to_crs("EPSG:4326")

    # Build a clean table: neighbourhood name + polygon as WKT
    out = hoods[["neighbourhood", "geometry"]].copy()
    out["geometry_wkt"] = out["geometry"].apply(lambda g: g.wkt)
    df = out[["neighbourhood", "geometry_wkt"]]

    # Load to BigQuery; geometry_wkt as a string, then we convert to GEOGRAPHY
    bq = bigquery.Client(project=PROJECT)
    staging = f"{PROJECT}.smart_stay.neighbourhood_geo_staging"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(df, staging, job_config=job_config).result()
    print("Staging loaded.")

    # Create final table with proper GEOGRAPHY type
    bq.query(f"""
        CREATE OR REPLACE TABLE `{TABLE}` AS
        SELECT
          neighbourhood,
          SAFE.ST_GEOGFROMTEXT(geometry_wkt) AS geometry,
          geometry_wkt
        FROM `{staging}`
    """).result()
    print(f"Created {TABLE} with GEOGRAPHY column.")

    #  Drop staging
    bq.query(f"DROP TABLE `{staging}`").result()
    print("Done.")

if __name__ == "__main__":
    run()