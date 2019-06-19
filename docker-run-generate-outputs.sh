#!/bin/bash

set -e

IMAGE_NAME=imaqal

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
if [[ $# -ne 24 ]]; then
    echo "Usage: ./docker-run-generate-outputs.sh
    [--profile-cpu <profile-output-path>]
    <user> <pipeline-configuration-file-path> <google-cloud-credentials-file-path> <phone-number-uuid-table-path>
    <s01e01-input-path> <s01e02-input-path> <s01e03-input-path> <s01e04-input-path> <s01e05-input-path>
    <s01e06-input-path> <s01e06-hormuud-recovered-input_path> <s01e07-input-path> <s01mag03-input-path>
    <s01mag04-input-path> <s01mag05-input-path> <s01-demog-input-path> <s01-follow-up-w2-input-path>
    <prev-coded-dir> <json-output-path> <icr-output-dir> <coded-output-dir> <messages-output-csv>
    <individuals-output-csv> <production-output-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
PIPELINE_CONFIGURATION=$2
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$3
INPUT_PHONE_UUID_TABLE=$4
INPUT_S01E01=$5
INPUT_S01E02=$6
INPUT_S01E03=$7
INPUT_S01E04=$8
INPUT_S01E05=$9
INPUT_S01E06=${10}
INPUT_S01E06_HORMUUD_RECOVERED=${11}
INPUT_S01E07=${12}
INPUT_S01MAG03=${13}
INPUT_S01MAG04=${14}
INPUT_S01MAG05=${15}
INPUT_S01_DEMOG=${16}
INPUT_S01_FOLLOW_UP_W2=${17}
PREV_CODED_DIR=${18}
OUTPUT_JSON=${19}
OUTPUT_ICR_DIR=${20}
OUTPUT_CODED_DIR=${21}
OUTPUT_MESSAGES_CSV=${22}
OUTPUT_INDIVIDUALS_CSV=${23}
OUTPUT_PRODUCTION_CSV=${24}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi

CMD="pipenv run $PROFILE_CPU_CMD python -u generate_outputs.py  \
    \"$USER\" /data/pipeline_configuration.json /credentials/google-cloud-credentials.json \
    /data/phone-number-uuid-table-input.json /data/s01e01-input.json /data/s01e02-input.json \
    /data/s01e03-input.json /data/s01e04-input.json /data/s01e05-input.json /data/s01e06-input.json \
    /data/s01e06_hormuud_recovered_input_path.json /data/s01e07-input.json /data/s01mag03-input.json /data/s01mag04-input.json \
    /data/s01mag05-input.json /data/s01-demog-input.json /data/s01-follow-up-w2-input.json \
    /data/prev-coded /data/output.json /data/output-icr /data/coded \
    /data/output-messages.csv /data/output-individuals.csv /data/output-production.csv \
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_PHONE_UUID_TABLE" "$container:/data/phone-number-uuid-table-input.json"
docker cp "$INPUT_S01E01" "$container:/data/s01e01-input.json"
docker cp "$INPUT_S01E02" "$container:/data/s01e02-input.json"
docker cp "$INPUT_S01E03" "$container:/data/s01e03-input.json"
docker cp "$INPUT_S01E04" "$container:/data/s01e04-input.json"
docker cp "$INPUT_S01E05" "$container:/data/s01e05-input.json"
docker cp "$INPUT_S01E06" "$container:/data/s01e06-input.json"
docker cp "$INPUT_S01E06_HORMUUD_RECOVERED" "$container:/data/s01e06_hormuud_recovered_input_path.json"
docker cp "$INPUT_S01E07" "$container:/data/s01e07-input.json"
docker cp "$INPUT_S01MAG03" "$container:/data/s01mag03-input.json"
docker cp "$INPUT_S01MAG04" "$container:/data/s01mag04-input.json"
docker cp "$INPUT_S01MAG05" "$container:/data/s01mag05-input.json"
docker cp "$INPUT_S01_DEMOG" "$container:/data/s01-demog-input.json"
docker cp "$INPUT_S01_FOLLOW_UP_W2" "$container:/data/s01-follow-up-w2-input.json"

if [[ -d "$PREV_CODED_DIR" ]]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"

mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

mkdir -p "$OUTPUT_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_CODED_DIR"

mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi
