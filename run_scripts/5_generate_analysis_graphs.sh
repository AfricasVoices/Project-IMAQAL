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

if [[ $# -ne 3 ]]; then
    echo "Usage: ./5_generate_analysis_graphs.sh [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path] <user> <pipeline-configuration-file-path> <data-root>"
    echo "Generates analysis graphs from individual and messages Traced-data"
    exit
fi

USER=$1
PIPELINE_CONFIGURATION=$2
DATA_ROOT=$3

mkdir -p "$DATA_ROOT/Coded Coda Files"
mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run-generate-analysis-graphs.sh ${CPU_PROFILE_ARG} ${MEMORY_PROFILE_ARG} \
    "$USER" "$DATA_ROOT/Outputs/messages_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/individuals_traced_data.jsonl" "$DATA_ROOT/Outputs/"
    
