#!/bin/bash

set -e

IMAGE_NAME=imaqal-upload-files

# Check that the correct number of arguments were provided.
if [[ $# -ne 10 ]]; then
    echo "Usage: ./docker-run-upload-files.sh
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id>
    <memory-profile-path> <data-archive-path> <production-input-csv>
    <messages-input-csv> <individuals-input-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
RUN_ID=$4
INPUT_PRODUCTION_FILE=$5
INPUT_MESSAGES_FILE=$6
INPUT_INDIVIDUALS_FILE=$7
INPUT_MEMORY_PROFILE=$8
INPUT_DATA_ARCHIVE=$9
PIPELINE_RUN_MODE=${10}

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u upload_files.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \"$RUN_ID\" \
    /data/input-production.csv /data/input-messages.csv /data/input-individuals.csv /data/memory.profile \
    /data/data-archive.tar.gzip \"$PIPELINE_RUN_MODE\"
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $INPUT_PIPELINE_CONFIGURATION -> $container_short_id:/data/pipeline_configuration.json"
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"

echo "Copying $INPUT_GOOGLE_CLOUD_CREDENTIALS -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"

echo "Copying $INPUT_PRODUCTION_FILE -> $container_short_id:/data/input-production.csv"
docker cp "$INPUT_PRODUCTION_FILE" "$container:/data/input-production.csv"

echo "Copying $INPUT_MEMORY_PROFILE -> $container_short_id:/data/memory.profile"
docker cp "$INPUT_MEMORY_PROFILE" "$container:/data/memory.profile"

echo "Copying $INPUT_DATA_ARCHIVE -> $container_short_id:/data/data-archive.tar.gzip"
docker cp "$INPUT_DATA_ARCHIVE" "$container:/data/data-archive.tar.gzip"

if [[ $PIPELINE_RUN_MODE = "all-stages" ]]; then
    echo "Copying $INPUT_MESSAGES_FILE -> $container_short_id:/data/input-messages.csv"
    docker cp "$INPUT_MESSAGES_FILE" "$container:/data/input-messages.csv"

    echo "Copying $INPUT_INDIVIDUALS_FILE -> $container_short_id:/data/input-individuals.csv"
    docker cp "$INPUT_INDIVIDUALS_FILE" "$container:/data/input-individuals.csv"
fi

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
