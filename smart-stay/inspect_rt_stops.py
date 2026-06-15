import requests
from google.transit import gtfs_realtime_pb2

resp = requests.get("https://production.gtfsrt.vbb.de/data", timeout=60)
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(resp.content)

# Look at the first few trip updates' stop_time_updates
count = 0
for entity in feed.entity:
    if entity.HasField("trip_update"):
        for stu in entity.trip_update.stop_time_update:
            sid = stu.stop_id if stu.HasField("stop_id") else "(no stop_id)"
            delay = stu.arrival.delay if (stu.HasField("arrival") and stu.arrival.HasField("delay")) else "(no delay)"
            print(f"stop_id={sid}  delay={delay}")
            count += 1
            if count >= 15:
                break
    if count >= 15:
        break