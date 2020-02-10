#!/usr/bin/env bash

set -e

IMAGE_NAME=imaqal-fetch-recovered-data

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: ./docker-run-fetch-recovered-data.sh
    [--profile-cpu <profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path>
    <raw-data-dir>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
RAW_DATA_DIR=$4

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run $PROFILE_CPU_CMD python -u fetch_recovered_data.py \
    \"$USER\" /credentials/google-cloud-credentials.json \
    /data/pipeline-configuration.json /data/Raw\ Data
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"
docker cp "$PIPELINE_CONFIGURATION" "$container:/data/pipeline-configuration.json"
mkdir -p "$RAW_DATA_DIR"
docker cp "$RAW_DATA_DIR/." "$container:/data/Raw Data/"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
docker cp "$container:/data/Raw Data/." "$RAW_DATA_DIR"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
