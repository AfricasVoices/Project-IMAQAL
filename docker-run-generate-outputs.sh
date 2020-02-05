#!/bin/bash

set -e

IMAGE_NAME=imaqal

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --profile-memory)
            PROFILE_MEMORY=true
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 16 ]]; then
    echo "Usage: ./docker-run-generate-outputs.sh
    [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <pipeline-run-mode> <raw-data-dir>
    <prev-coded-dir>  <icr-output-dir> <coded-output-dir> <production-output-csv> <auto-coding-json-output-path>
    <messages-output-csv> <individuals-output-csv> <messages-json-output-path> <individual-json-output-path>
    <messages-history-json-output-path> <individuals-history-json-output-path>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
PIPELINE_RUN_MODE=$4
INPUT_RAW_DATA_DIR=$5
PREV_CODED_DIR=$6
OUTPUT_ICR_DIR=$7
OUTPUT_CODED_DIR=$8
OUTPUT_PRODUCTION_CSV=$9
OUTPUT_AUTO_CODING_TRACED_JSONL=${10}
OUTPUT_MESSAGES_CSV=${11}
OUTPUT_INDIVIDUALS_CSV=${12}
OUTPUT_MESSAGES_JSONL=${13}
OUTPUT_INDIVIDUALS_JSONL=${14}
OUTPUT_MESSAGES_HISTORY_JSONL=${15}
OUTPUT_INDIVIDUALS_HISTORY_JSONL=${16}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" --build-arg INSTALL_MEMORY_PROFILER="$PROFILE_MEMORY" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="-m pyinstrument -o /data/cpu.prof --renderer html -- "
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
if [[ "$PROFILE_MEMORY" = true ]]; then
    PROFILE_MEMORY_CMD="mprof run -o /data/memory.prof"
fi

CMD="pipenv run $PROFILE_MEMORY_CMD python -u $PROFILE_CPU_CMD generate_outputs.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json "$PIPELINE_RUN_MODE" \
    /data/raw-data /data/prev-coded /data/output-icr /data/coded /data/output-production.csv \
    /data/auto-coding-traced-data.jsonl /data/output-messages.csv /data/output-individuals.csv \
    /data/output-messages.jsonl /data/output-individuals.jsonl \
    /data/output-messages-history.jsonl /data/output-individuals-history.jsonl
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"
docker cp "$PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$INPUT_RAW_DATA_DIR" "$container:/data/raw-data"

if [[ -d "$PREV_CODED_DIR" ]]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
echo "copying icr files to "$OUTPUT_ICR_DIR" "
mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

echo "copying auto-coded messages to "$OUTPUT_CODED_DIR" "
mkdir -p "$OUTPUT_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_CODED_DIR"

echo "copying production file to "$OUTPUT_PRODUCTION_CSV" "
mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

if [[ $PIPELINE_RUN_MODE = "all-stages" ]]; then
    echo "copying output-messages.csv to "$OUTPUT_MESSAGES_CSV" "
    mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
    docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

    echo "copying output-individuals.csv to "$OUTPUT_INDIVIDUALS_CSV" "
    mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
    docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

    echo "copying traced messages.jsonl to "$OUTPUT_MESSAGES_JSONL" "
    mkdir -p "$(dirname "$OUTPUT_MESSAGES_JSONL")"
    docker cp "$container:/data/output-messages.jsonl" "$OUTPUT_MESSAGES_JSONL"

    echo "copying traced individuals.jsonl to "$OUTPUT_INDIVIDUALS_JSONL" "
    mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_JSONL")"
    docker cp "$container:/data/output-individuals.jsonl" "$OUTPUT_INDIVIDUALS_JSONL"

    echo "copying traced messages history.jsonl to $OUTPUT_MESSAGES_HISTORY_JSONL"
    mkdir -p "$(dirname "$OUTPUT_MESSAGES_HISTORY_JSONL")"
    docker cp "$container:/data/output-messages-history.jsonl" "$OUTPUT_MESSAGES_HISTORY_JSONL"

    echo "copying traced individuals history.jsonl to $OUTPUT_INDIVIDUALS_HISTORY_JSONL"
    mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_HISTORY_JSONL")"
    docker cp "$container:/data/output-individuals-history.jsonl" "$OUTPUT_INDIVIDUALS_HISTORY_JSONL"

elif [[ $PIPELINE_RUN_MODE = "auto-code-only" ]]; then
    echo "copying auto-coding-traced-data.jsonl to "$OUTPUT_AUTO_CODING_TRACED_JSONL" "
    mkdir -p "$(dirname "$OUTPUT_AUTO_CODING_TRACED_JSONL")"
    docker cp "$container:/data/auto-coding-traced-data.jsonl" "$OUTPUT_AUTO_CODING_TRACED_JSONL"
fi

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

if [[ "$PROFILE_MEMORY" = true ]]; then
    mkdir -p "$(dirname "$MEMORY_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/memory.prof" "$MEMORY_PROFILE_OUTPUT_PATH"
fi
