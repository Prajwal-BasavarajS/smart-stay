# clean raw Inside Airbnb listings into berlin_clean
import pandas as pd
from google.cloud import storage

BUCKET = "smart-stay-data"
RAW_BLOB = "raw/airbnb_listings.csv.gz"
OUT_BLOB = "berlin_clean.csv"

def clean_airbnb():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    # 1. Download raw gzipped listings to local temp
    local_raw = "/tmp/airbnb_listings.csv.gz"
    bucket.blob(RAW_BLOB).download_to_filename(local_raw)

    # pandas reads .gz directly (compression inferred from extension)
    df = pd.read_csv(local_raw, compression="gzip", low_memory=False)
    print("Shape (rows, columns):", df.shape)

    # 2. Keep only the columns our project needs
    keep = [
        "id", "latitude", "longitude",
        "neighbourhood_cleansed", "neighbourhood_group_cleansed",
        "room_type", "property_type", "accommodates", "bedrooms",
        "price",
        "review_scores_rating", "review_scores_location", "number_of_reviews",
        "estimated_occupancy_l365d", "estimated_revenue_l365d",
    ]
    df = df[keep]

    # 3. Clean the price: "$1,234.00" (text) -> number
    df["price"] = (
        df["price"]
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # 4. Mark unrealistic prices as missing (keep rows, null the bad price)
    df.loc[(df["price"] < 10) | (df["price"] > 1000), "price"] = None

    # 5. Summary
    print("Total listings kept:", len(df))
    print("Listings with valid price:", df["price"].notna().sum())
    print("Median price (EUR):", round(df["price"].median(), 2))
    print("Unique neighbourhoods:", df["neighbourhood_cleansed"].nunique())

    # 6. Upload cleaned CSV to GCS
    bucket.blob(OUT_BLOB).upload_from_string(df.to_csv(index=False),
                                             content_type="text/csv")
    print(f"Saved cleaned data to gs://{BUCKET}/{OUT_BLOB}")

if __name__ == "__main__":
    clean_airbnb()