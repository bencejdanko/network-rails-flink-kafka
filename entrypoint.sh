#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Entrypoint: Running as user $(whoami) (UID $(id -u), GID $(id -g))"
echo "Entrypoint: Received arguments: $@"
echo "Entrypoint: Mounting JuiceFS..."

# Export AWS-style credentials if JUICEFS variables are set
if [ -n "${AWS_ACCESS_KEY_ID}" ] && [ -n "${AWS_SECRET_ACCESS_KEY}" ]; then
  echo "AWS credentials entered."
else
  echo "Entrypoint: Warning - AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set."
  exit 1
fi

# Check if already mounted (e.g., container restart)
if mountpoint -q /mnt/jfs; then
  echo "Entrypoint: /mnt/jfs is already mounted."
else
  echo "Entrypoint: Attempting JuiceFS mount..."
  # Add --verbose or -v for more mount logging during debugging
  # Consider adding --log /path/to/juicefs.log if persistent logs are needed
  juicefs mount -v --background \
    "${JUICEFS_META_URL}" \
    /mnt/jfs

  echo "Waiting for JuiceFS mount to stabilize..."
  sleep 5 # Wait for the mount to stabilize

  # Verify the mount succeeded
  if mountpoint -q /mnt/jfs; then
    echo "Entrypoint: JuiceFS successfully mounted on /mnt/jfs."
    # Optional: Perform a quick read/write test if needed
    # touch /mnt/jfs/.mount_test && rm /mnt/jfs/.mount_test
  else
    echo "Entrypoint: ERROR - Failed to mount JuiceFS on /mnt/jfs!" >&2
    echo "Entrypoint: Check container logs and JuiceFS logs (if configured) for details." >&2
    # Optional: Dump JuiceFS log if configured
    # [[ -f /path/to/juicefs.log ]] && cat /path/to/juicefs.log
    exit 1 # Exit if mount failed
  fi
fi

# ----- Execute the original Flink entrypoint -----
# This script knows how to handle "jobmanager", "taskmanager", etc.
echo "Entrypoint: Executing original Flink entrypoint (/docker-entrypoint.sh) with arguments: $@"
exec /docker-entrypoint.sh "$@"