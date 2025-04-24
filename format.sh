set -e # Exit immediately if a command exits with a non-zero status

set -a  # Automatically export all variables
source <(grep -vE '^#|^UID=' .env) || true
set +a

VOLUME_NAME="rails" 

echo "Current variables:"
echo "------------------------------------------------------"
echo "VOLUME_NAME: ${VOLUME_NAME}"
echo "JUICEFS_META_URL: ${JUICEFS_META_URL}"
echo "AWS_URI: ${AWS_URI}"
echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}"
echo "AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}"
echo "------------------------------------------------------"
echo "Running JuiceFS format command for volume '${VOLUME_NAME}'..."
echo "------------------------------------------------------"


docker compose run --rm \
  --entrypoint "" \
  jobmanager \
  juicefs format \
  --storage s3 \
  --bucket ${AWS_URI} \
  --access-key ${AWS_ACCESS_KEY_ID} \
  --secret-key ${AWS_SECRET_ACCESS_KEY} \
  ${JUICEFS_META_URL} \
  ${VOLUME_NAME} 

# Check the exit code of the docker compose run command
if [ $? -eq 0 ]; then
  echo "------------------------------------------------------"
  echo "JuiceFS format command executed successfully for volume '${VOLUME_NAME}'."
  echo "You should now be able to start your services:"
  echo "docker compose up -d"
  echo "------------------------------------------------------"
else
  echo "------------------------------------------------------"
  echo "ERROR: JuiceFS format command failed."
  echo "Check the output above for details."
  echo "------------------------------------------------------"
  exit 1
fi