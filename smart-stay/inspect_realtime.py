import requests
from google.transit import gtfs_realtime_pb2
from datetime import datetime

RT_URL = "https://production.gtfsrt.vbb.de/data"

def inspect():
    resp = requests.get(RT_URL, timeout=60)
    resp.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    print(f"Feed fetched at: {datetime.now().strftime('%H:%M:%S')}")
    print(f"Feed timestamp: {feed.header.timestamp}")
    print(f"Total entities in feed: {len(feed.entity)}\n")

    trip_updates = 0
    vehicle_positions = 0
    delays = []

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_updates += 1
            for stu in entity.trip_update.stop_time_update:
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    delays.append(stu.arrival.delay)
        if entity.HasField("vehicle"):
            vehicle_positions += 1

    print(f"Trip updates (delays/changes): {trip_updates}")
    print(f"Vehicle positions (live GPS):  {vehicle_positions}")

    if delays:
        on_time = sum(1 for d in delays if abs(d) <= 60)
        late    = sum(1 for d in delays if d > 60)
        early   = sum(1 for d in delays if d < -60)
        print(f"\nDelay snapshot ({len(delays)} stop updates):")
        print(f"  On time (±1 min): {on_time}")
        print(f"  Late (>1 min):    {late}")
        print(f"  Early (>1 min):   {early}")
        print(f"  Max delay: {max(delays)//60} min, "
              f"Min: {min(delays)//60} min")

    #  live trips
    print("\nSample live trips:")
    for entity in feed.entity[:5]:
        if entity.HasField("trip_update"):
            tu = entity.trip_update
            print(f"  Trip {tu.trip.trip_id[:25]}... "
                  f"route {tu.trip.route_id}, "
                  f"{len(tu.stop_time_update)} stop updates")

if __name__ == "__main__":
    inspect()