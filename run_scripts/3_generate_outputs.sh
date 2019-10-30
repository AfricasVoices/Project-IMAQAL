#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            CPU_PROFILE_OUTPUT_PATH="$2"
            CPU_PROFILE_ARG="--profile-cpu $CPU_PROFILE_OUTPUT_PATH"
            shift 2;;
        --profile-memory)
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            MEMORY_PROFILE_ARG="--profile-memory $MEMORY_PROFILE_OUTPUT_PATH"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 5 ]]; then
    echo "Usage: ./3_generate_outputs.sh [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path] <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <pipeline-run-mode> <data-root>"
    echo "Generates the outputs needed downstream from raw data files generated by step 2 and uploads to Google Drive"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
PIPELINE_RUN_MODE=$4
DATA_ROOT=$5

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run-generate-outputs.sh ${CPU_PROFILE_ARG} ${MEMORY_PROFILE_ARG} \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$PIPELINE_RUN_MODE" \
    "$DATA_ROOT/Raw Data" "$DATA_ROOT/Coded Coda Files/" "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/imaqal_production.csv" "$DATA_ROOT/Outputs/auto_coding_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/imaqal_messages.csv" "$DATA_ROOT/Outputs/imaqal_individuals.csv" \
    "$DATA_ROOT/Outputs/messages_traced_data.jsonl" "$DATA_ROOT/Outputs/individuals_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/messages_traced_data_history.jsonl" "$DATA_ROOT/Outputs/individuals_traced_data_history_jsonl"
