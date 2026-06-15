import requests
from google.cloud import storage, secretmanager
import pandas as pd

PROJECT = "stay-smart-498122"
BUCKET = "smart-stay-data"
DEST_BLOB = "raw/berlin_events.csv"

def get_api_key():
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT}/secrets/ticketmaster-key/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")

def fetch_events():
    api_key = get_api_key()
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": api_key,
        "city": "Berlin",
        "countryCode": "DE",
        "size": 200,            
        "sort": "date,asc",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    events = []
    for ev in data.get("_embedded", {}).get("events", []):
        venue = ev.get("_embedded", {}).get("venues", [{}])[0]
        loc = venue.get("location", {})
        events.append({
            "name": ev.get("name"),
            "date": ev.get("dates", {}).get("start", {}).get("localDate"),
            "venue": venue.get("name"),
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
            "category": (ev.get("classifications", [{}])[0]
                         .get("segment", {}).get("name")),
        })

    df = pd.DataFrame(events)
    print(f"Fetched {len(df)} events")

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    bucket.blob(DEST_BLOB).upload_from_string(df.to_csv(index=False),
                                              content_type="text/csv")
    print(f"Uploaded to gs://{BUCKET}/{DEST_BLOB}")

if __name__ == "__main__":
    fetch_events()