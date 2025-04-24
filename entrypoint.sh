#!/bin/bash
set -e

echo "Mounting JuiceFS..."

# Export AWS-style credentials
export AWS_ACCESS_KEY_ID=${JUICEFS_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${JUICEFS_SECRET_KEY}

# Mount JuiceFS
juicefs mount redis://redis:6379/1 /mnt/jfs \
  --storage ${JUICEFS_STORAGE} \
  --bucket https://s3.amazonaws.com/${JUICEFS_BUCKET} &

sleep 5

echo "Starting Flink Job: $@"
exec "$@"
