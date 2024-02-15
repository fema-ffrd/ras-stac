#!/bin/bash

# Test script to build and run the docker container.
CONTAINER_NAME="ras-stac"
CMD="python3 -m new_ras_geom"

# Path to your JSON configuration file
CONFIG_JSON_PATH="ras-stac/examples/ras-geom.json"

docker build -f Dockerfile -t $CONTAINER_NAME . # --no-cache

# Extract JSON from file
JSON_STRING=$(jq -c . "$CONFIG_JSON_PATH")
if [ $? -ne 0 ]; then
    echo "Failed to parse JSON from $CONFIG_JSON_PATH"
    exit 1
fi

# Run the Docker container with the JSON string
docker run --rm -it --env-file $(pwd)/.env $CONTAINER_NAME:latest $CMD "$JSON_STRING"

echo "Process completed and container removed."