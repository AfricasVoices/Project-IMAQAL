#!/usr/bin/env bash

set -e

if [[ $# -ne 10 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <google-cloud-credentials-file-path> <pipeline-configuration-json> <pipeline-run-mode>"
    echo "  <coda-pull-credentials-path> <coda-push-credentials-path>"
    echo "  <coda-tools-root> <data-root> <data-backup-dir> <performance-logs-dir>"
    echo "Runs the pipeline end-to-end (coda fetch, raw-data fetch, recovered-raw-data fetch,output generation, Drive upload, Coda upload, data backup)"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION=$3
PIPELINE_RUN_MODE=$4
CODA_PULL_CREDENTIALS_PATH=$5
CODA_PUSH_CREDENTIALS_PATH=$6
CODA_TOOLS_ROOT=$7
DATA_ROOT=$8
DATA_BACKUPS_DIR=$9
PERFORMANCE_LOGS_DIR=${10}

RUN_ID=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

#./1_coda_get.sh "$CODA_PULL_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

#./2_fetch_data.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

#./3_generate_outputs.sh --profile-memory "$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile" \
    #"$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$PIPELINE_RUN_MODE" "$DATA_ROOT"

./4_coda_add.sh "$CODA_PUSH_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

#if [[ $PIPELINE_RUN_MODE == "all-stages" ]]; then
    #./5_generate_analysis_graphs.sh --profile-memory "$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile" \
        #"$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"
#fi

#./6_backup_data_root.sh "$DATA_ROOT" "$DATA_BACKUPS_DIR/data-$RUN_ID.tar.gzip"

./7_upload_files.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION" "$RUN_ID" \
    "$DATA_ROOT" "$PERFORMANCE_LOGS_DIR/memory-$RUN_ID.profile" "$DATA_BACKUPS_DIR/data-$RUN_ID.tar.gzip"  \
    "$PIPELINE_RUN_MODE"
