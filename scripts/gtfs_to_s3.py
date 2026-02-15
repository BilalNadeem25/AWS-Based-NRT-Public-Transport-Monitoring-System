#!/usr/bin/env python3

import json
import os
from datetime import datetime, timezone
from requests import get
import boto3
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

# ---------------- CONFIG ----------------
GTFS_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl"
S3_BUCKET = "gtfs-s3"
S3_PREFIX = "input"
LOCAL_TMP_DIR = "/tmp"
TIMEOUT = 10  # seconds
# ----------------------------------------

def fetch_vehicle_positions():
    feed = gtfs_realtime_pb2.FeedMessage()
    response = get(GTFS_URL, timeout=TIMEOUT)
    response.raise_for_status()
    feed.ParseFromString(response.content)

    vehicles = [
        MessageToDict(entity.vehicle, preserving_proto_field_name=True)
        for entity in feed.entity
        if entity.HasField("vehicle")
    ]

    return vehicles


def save_local_json(data, timestamp):
    filename = f"vehicle_positions_{timestamp}.json"
    local_path = os.path.join(LOCAL_TMP_DIR, filename)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return local_path, filename


def upload_to_s3(local_path, filename):
    s3 = boto3.client("s3")

    s3_key = f"{S3_PREFIX}/{filename}"
    s3.upload_file(local_path, S3_BUCKET, s3_key)

    return f"s3://{S3_BUCKET}/{s3_key}"


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    vehicles = fetch_vehicle_positions()
    print(f"[INFO] Vehicles fetched: {len(vehicles)}")

    local_path, filename = save_local_json(vehicles, timestamp)
    print(f"[INFO] Saved locally: {local_path}")

    s3_uri = upload_to_s3(local_path, filename)
    print(f"[INFO] Uploaded to: {s3_uri}")


if __name__ == "__main__":
    main()
