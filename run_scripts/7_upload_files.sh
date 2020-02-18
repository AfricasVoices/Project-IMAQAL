#!/usr/bin/env bash

set -e

if [[ $# -ne 8 ]]; then
    echo "Usage: ./7_upload_logs <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id>
         <memory-profile-file-path> <data-archive-file-path> <data-dir> <pipeline-run-mode>  "
    echo "Uploads the pipeline logs"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
DATA_ROOT=$5
MEMORY_PROFILE_FILE_PATH=$6
DATA_ARCHIVE_FILE_PATH=$7
PIPELINE_RUN_MODE=$8

cd ..
./docker-run-upload-files.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$RUN_ID" "$DATA_ROOT/Outputs/imaqal_production.csv" "$DATA_ROOT/Outputs/imaqal_messages.csv" \
     "$DATA_ROOT/Outputs/imaqal_individuals.csv" "$MEMORY_PROFILE_FILE_PATH" "$DATA_ARCHIVE_FILE_PATH" "$PIPELINE_RUN_MODE" \
