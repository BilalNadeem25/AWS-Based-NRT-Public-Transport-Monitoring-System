#!/bin/bash

BUCKET="gtfs-s3"
INPUT_PREFIX="input"
CLUSTER_ID="j-ZTJVTO2F08IC"

# ----------------------------------------
# Find latest GTFS file
# ----------------------------------------
LATEST_FILE=$(aws s3 ls s3://$BUCKET/$INPUT_PREFIX/ \
  | grep vehicle_positions_ \
  | sort \
  | tail -n 1 \
  | awk '{print $4}')

if [ -z "$LATEST_FILE" ]; then
  echo "No GTFS file found. Exiting."
  exit 1
fi

echo "Latest GTFS file: $LATEST_FILE"

# ----------------------------------------
# Submit Spark job to EMR
# ----------------------------------------
aws emr add-steps \
--cluster-id $CLUSTER_ID \
--steps Type=Spark,Name="GTFS-Batch-$(date +%Y%m%d%H%M%S)",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,s3://gtfs-s3/scripts/spark_gtfs_emr_final.py,s3://$BUCKET/$INPUT_PREFIX/$LATEST_FILE]
