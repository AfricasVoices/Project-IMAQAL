#!/bin/bash

set -e

IMAGE_NAME=imaqal-upload-files

# Check that the correct number of arguments were provided.
if [[ $# -ne 10 ]]; then
    echo "Usage: ./docker-run-upload-logs.sh
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
INPUT_MEMORY_PROFILE=$5
INPUT_DATA_ARCHIVE=$6
INPUT_PRODUCTION_FILE=$7
INPUT_MESSAGES_FILE=$8
INPUT_INDIVIDUALS_FILE=$9
PIPELINE_RUN_MODE=${10}

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u upload_files.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \
    \"$RUN_ID\" /data/memory.profile /data/data-archive.tar.gzip /data/input-production.csv \
    /data/input-messages.csv /data/input-individuals.csv "$PIPELINE_RUN_MODE" \
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_MEMORY_PROFILE" "$container:/data/memory.profile"
docker cp "$INPUT_DATA_ARCHIVE" "$container:/data/data-archive.tar.gzip"
docker cp "$INPUT_PRODUCTION_FILE" "$container:/data/input-production.csv"
docker cp "$INPUT_MESSAGES_FILE" "$container:/data/input-messages.csv"
docker cp "$INPUT_INDIVIDUALS_FILE" "$container:/data/input-individuals.csv"

# Run the container
docker start -a -i "$container"

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
