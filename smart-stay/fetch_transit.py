# fetch_transit.py — download VBB GTFS static files + realtime feed to GCS (large-file safe)
import requests
from google.cloud import storage

BUCKET = "smart-stay-data"
STATIC_BASE = "https://vbb-gtfs.jannisr.de/latest"
RT_URL = "https://production.gtfsrt.vbb.de/data"

STATIC_FILES = ["stops.csv", "stop_times.csv", "trips.csv", "routes.csv", "calendar.csv"]

def download_to_file(url, local_path):
    """Stream-download a URL to a local file (no large in-memory buffer)."""
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB chunks
                f.write(chunk)

def fetch_transit():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    print("Downloading static GTFS files...")
    for fname in STATIC_FILES:
        print(f"  fetching {fname} ...")
        local = f"/tmp/{fname}"
        download_to_file(f"{STATIC_BASE}/{fname}", local)
        blob = bucket.blob(f"gtfs/{fname}")
        blob.upload_from_filename(local, timeout=600)
        print(f"  -> gs://{BUCKET}/gtfs/{fname}")

    print("Downloading realtime GTFS-RT feed...")
    download_to_file(RT_URL, "/tmp/realtime.pb")
    bucket.blob("gtfs/realtime.pb").upload_from_filename("/tmp/realtime.pb", timeout=120)
    print(f"  -> gs://{BUCKET}/gtfs/realtime.pb")

    print("Transit fetch complete.")

if __name__ == "__main__":
    fetch_transit()