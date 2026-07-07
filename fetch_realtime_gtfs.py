import datetime
import os
import time
import io
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from google.transit import gtfs_realtime_pb2


def upload_df_to_s3(df: pd.DataFrame, bucket_name: str, s3_key: str):
    """
    Converts a Pandas DataFrame to an in-memory CSV and uploads it directly to S3.
    No local files are written to disk.
    """
    try:
        # Initialize the S3 client
        # boto3 automatically looks for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables
        s3_client = boto3.client("s3")
        
        # Write DataFrame directly to a string buffer instead of a physical file
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        print(f"📤 Uploading data to s3://{bucket_name}/{s3_key}...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print("🎉 Upload successful!")
        
    except NoCredentialsError:
        print("❌ AWS credentials not found. Make sure environment variables are configured properly.")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")


def collect_realtime_gtfs_data(
    duration_minutes: int = 5,
    interval_seconds: int = 60,
    bucket_name: str = "your-mta-gtfs-bucket-name"  # Replace with your actual S3 bucket name
):
    """
    Collect public real-time GTFS-RT data from MTA Bus for a given duration,
    and pipe the processed output directly to S3.
    """
    REALTIME_URL = "https://gtfsrt.prod.obanyc.com/tripUpdates"
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
                print(f"✅ {len(batch_records)} records fetched | Total accumulated: {len(all_records)}")

            time.sleep(interval_seconds)

        except Exception as e:
            print(f"⚠️ Error during extraction: {e}")
            time.sleep(interval_seconds)

    if all_records:
        df = pd.DataFrame(all_records)
        
        # Partitioning pattern: organizing files by year/month/day folder hierarchy makes ETL easier later
        now = datetime.datetime.now()
        date_folder = now.strftime("year=%Y/month=%m/day=%d")
        time_str = now.strftime("%H-%M")
        s3_key = f"mta_bus/{date_folder}/gtfs_data_{time_str}.csv"
        
        # Trigger the S3 upload
        upload_df_to_s3(df, bucket_name, s3_key)
    else:
        print("⚠️ No data collected during this run.")


if __name__ == "__main__":
    # Ensure your bucket name matches your AWS bucket setup
    BUCKET = os.getenv("MTA_S3_BUCKET_NAME", "your-mta-gtfs-bucket-name")
    collect_realtime_gtfs_data(duration_minutes=5, interval_seconds=60, bucket_name=BUCKET)
