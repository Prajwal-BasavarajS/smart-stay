import requests
from google.cloud import storage

# Config
SNAPSHOT_DATE = "2025-09-23"   # update per Inside Airbnb release
CITY_PATH = "germany/be/berlin"
URL = f"https://data.insideairbnb.com/{CITY_PATH}/{SNAPSHOT_DATE}/data/listings.csv.gz"

BUCKET = "smart-stay-data"
DEST_BLOB = "raw/airbnb_listings.csv.gz"

def fetch_airbnb():
    print(f"Downloading: {URL}")
    resp = requests.get(URL, timeout=120)
    resp.raise_for_status()                 # fails loudly if URL is dead
    print(f"Downloaded {len(resp.content)/1e6:.1f} MB")

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(DEST_BLOB)
    blob.upload_from_string(resp.content, content_type="application/gzip")
    print(f"Uploaded to gs://{BUCKET}/{DEST_BLOB}")

if __name__ == "__main__":
    fetch_airbnb()