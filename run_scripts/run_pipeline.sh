#!/usr/bin/env bash

set -e

if [[ $# -ne 9 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <google-cloud-credentials-file-path> <pipeline-configuration-json>"
    echo "  <coda-pull-credentials-path> <coda-push-credentials-path>"
    echo "  <coda-tools-root> <data-root> <data-backup-dir> <performance-logs-dir>"
    echo "Runs the pipeline end-to-end (coda fetch, raw-data fetch, recovered-raw-data fetch,output generation, Drive upload, Coda upload, data backup)"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
CODA_PULL_CREDENTIALS_PATH=$4
CODA_PUSH_CREDENTIALS_PATH=$5
CODA_TOOLS_ROOT=$6
DATA_ROOT=$7
DATA_BACKUPS_DIR=$8
PERFORMANCE_LOGS_DIR=$9

RUN_ID=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

#./1_coda_get.sh "$CODA_PULL_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

#./2_fetch_data.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./3_generate_outputs.sh --profile-memory "$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile" \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

#./4_coda_add.sh "$CODA_PUSH_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

#./5_backup_data_root.sh "$DATA_ROOT" "$DATA_BACKUPS_DIR" "$RUN_ID"

#./6_upload_logs.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$RUN_ID" \
    #"$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile"