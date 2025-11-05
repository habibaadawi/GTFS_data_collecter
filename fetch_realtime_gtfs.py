import requests
import datetime
import pandas as pd
import time
import os
from google.transit import gtfs_realtime_pb2  # You’ll install this

def collect_realtime_gtfs_data(
    duration_minutes: int = 5,
    interval_seconds: int = 60,
    output_dir: str = "data"
):
    """
    Collect public real-time GTFS-RT data from MTA Bus (no API key required)
    for a given duration, saving each batch to CSV.
    """

    REALTIME_URL = "https://gtfsrt.prod.obanyc.com/tripUpdates"
    os.makedirs(output_dir, exist_ok=True)

    all_records = []
    start_time = time.time()
    collection_end_time = start_time + (duration_minutes * 60)

    print(f"🚀 Starting GTFS collection for {duration_minutes} minutes...")
    print(f"📡 Fetching every {interval_seconds} seconds")

    while time.time() < collection_end_time:
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            response = requests.get(REALTIME_URL, timeout=10)
            response.raise_for_status()
            feed.ParseFromString(response.content)

            batch_records = []
            current_timestamp = datetime.datetime.now()

            for entity in feed.entity:
                if not entity.HasField("trip_update"):
                    continue

                trip_id = entity.trip_update.trip.trip_id
                route_id = entity.trip_update.trip.route_id

                for stu in entity.trip_update.stop_time_update:
                    record = {
                        "timestamp": current_timestamp,
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "stop_id": stu.stop_id,
                        "arrival_time": datetime.datetime.fromtimestamp(stu.arrival.time)
                        if stu.arrival.HasField("time") else None,
                        "departure_time": datetime.datetime.fromtimestamp(stu.departure.time)
                        if stu.departure.HasField("time") else None,
                        "arrival_delay": stu.arrival.delay if stu.arrival.HasField("delay") else None,
                        "departure_delay": stu.departure.delay if stu.departure.HasField("delay") else None
                    }
                    batch_records.append(record)

            if batch_records:
                all_records.extend(batch_records)
                print(f"✅ {len(batch_records)} records fetched | Total: {len(all_records)}")

            time.sleep(interval_seconds)

        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(interval_seconds)

    if all_records:
        df = pd.DataFrame(all_records)
        date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = os.path.join(output_dir, f"gtfs_data_{date_str}.csv")
        df.to_csv(filename, index=False)
        print(f"💾 Saved {len(df)} records → {filename}")
    else:
        print("⚠️ No data collected.")


if __name__ == "__main__":
    collect_realtime_gtfs_data()
