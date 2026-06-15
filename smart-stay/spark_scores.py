from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "gs://smart-stay-data"
PROJECT = "stay-smart-498122"
DATASET = "smart_stay"
TEMP_BUCKET = "smart-stay-data"

def write_to_bq(df, table):
    (df.write.format("bigquery")
       .option("table", f"{PROJECT}.{DATASET}.{table}")
       .option("temporaryGcsBucket", TEMP_BUCKET)
       .mode("overwrite")
       .save())
    print(f"Wrote {table} ({df.count()} rows)")

def main():
    spark = SparkSession.builder.appName("SmartStayScores").getOrCreate()

    #  TRANSIT: clean + aggregate departures per stop/hour 
    stop_times = spark.read.csv(f"{BUCKET}/gtfs/stop_times.csv", header=True)
    stops      = spark.read.csv(f"{BUCKET}/gtfs/stops.csv", header=True)
    print("Raw stop_times rows:", stop_times.count())

    st = stop_times.select("trip_id", "stop_id", "departure_time")
    st = st.filter(F.col("departure_time").isNotNull() & (F.col("departure_time") != ""))
    st = st.withColumn("raw_hour", F.split(F.col("departure_time"), ":").getItem(0).cast("int"))
    st = st.withColumn("hour", F.col("raw_hour") % 24)
    st = st.filter(F.col("stop_id").startswith("de:11000:"))   # Berlin only
    print("Berlin departures after cleaning:", st.count())

    dep_per_stop_hour = st.groupBy("stop_id", "hour").agg(F.count("*").alias("departures"))

    stops_clean = stops.select(
        "stop_id",
        F.col("stop_lat").cast("double").alias("lat"),
        F.col("stop_lon").cast("double").alias("lon"),
    )
    departures = dep_per_stop_hour.join(stops_clean, on="stop_id", how="left")

    #  load STATIC reference lookups 
    lookup = spark.read.csv(f"{BUCKET}/processed/stop_to_neighbourhood.csv", header=True)
    areas  = spark.read.csv(f"{BUCKET}/processed/neighbourhood_areas.csv", header=True)

    # departures per neighbourhood 
    joined = departures.join(lookup, on="stop_id", how="inner")
    daily = (joined
        .withColumn("departures", F.col("departures").cast("int"))
        .groupBy("neighbourhood")
        .agg(F.sum("departures").alias("daily_departures")))

    # unique stations per neighbourhood (collapse platforms via regex)
    stations = (lookup
        .withColumn("station_id", F.regexp_extract(F.col("stop_id"), r"(de:\d+:\d+)", 1))
        .select("neighbourhood", "station_id").distinct()
        .groupBy("neighbourhood").agg(F.count("*").alias("num_stations")))

    areas = areas.withColumn("area_km2", F.col("area_km2").cast("double"))

    conn = daily.join(stations, "neighbourhood", "inner").join(areas, "neighbourhood", "inner")
    conn = conn.withColumn("dep_per_station", F.col("daily_departures") / F.col("num_stations"))
    conn = conn.withColumn("dep_per_km2", F.col("daily_departures") / F.col("area_km2"))

    def scale(frame, col):
        s = frame.agg(F.min(col).alias("mn"), F.max(col).alias("mx")).collect()[0]
        return frame.withColumn(col + "_score",
            F.round((F.col(col) - F.lit(s["mn"])) / (F.lit(s["mx"]) - F.lit(s["mn"])) * 100, 1))

    conn = scale(conn, "dep_per_station")
    conn = scale(conn, "dep_per_km2")
    conn = conn.withColumn("connectivity_score",
        F.round((F.col("dep_per_station_score") + F.col("dep_per_km2_score")) / 2, 1))

    connectivity = conn.select("neighbourhood", "daily_departures",
                               "num_stations", "area_km2", "connectivity_score")
    write_to_bq(connectivity, "connectivity_score")

    # VALUE: Airbnb median price 
    df = spark.read.csv(f"{BUCKET}/berlin_clean.csv", header=True)
    df = df.withColumn("price", F.col("price").cast("double"))

    hood = (df.filter(F.col("price").isNotNull())
        .groupBy("neighbourhood_cleansed")
        .agg(F.count("*").alias("num_listings"),
             F.round(F.expr("percentile_approx(price, 0.5)"), 2).alias("median_price"))
        .withColumnRenamed("neighbourhood_cleansed", "neighbourhood"))

    hood = hood.filter(F.col("num_listings") >= 30)
    s = hood.agg(F.min("median_price").alias("mn"), F.max("median_price").alias("mx")).collect()[0]
    value = hood.withColumn("value_score",
        F.round(100 - (F.col("median_price") - F.lit(s["mn"])) / (F.lit(s["mx"]) - F.lit(s["mn"])) * 100, 1))

    write_to_bq(value, "value_score")

    print("spark_scores complete.")
    spark.stop()

if __name__ == "__main__":
    main()