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

if [[ $# -ne 4 ]]; then
    echo "Usage: ./5_generate_analysis_graphs.sh [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path] <user> <google-cloud_credentials-file-path> <pipeline-configuration-file-path> <data-root>"
    echo "Generates analysis graphs from individual and messages Traced-data"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
DATA_ROOT=$4

mkdir -p "$DATA_ROOT/Outputs/analysis_graphs"

cd ..
./docker-run-generate-analysis-graphs.sh ${CPU_PROFILE_ARG} ${MEMORY_PROFILE_ARG} \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT/Outputs/messages_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/individuals_traced_data.jsonl" "$DATA_ROOT/Outputs/analysis_graphs"
