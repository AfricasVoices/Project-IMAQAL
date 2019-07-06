#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="IMAQAL"
DATASETS=(
    "s01e01"
    "s01e02"
    "s01e03"
    "s01e04"
    "s01e05"
    "s01e06"
    "s01e07"
    "s01mag03"
    "s01mag04"
    "s01mag05"
    "s01mag06"
    "s01mag07"
    "s01mag08"
    "s01mag09"
    "s01mag10"
    "s01mag11"
    "s01mag12"
    "s01mag13"
    "s01mag14"
    "s01mag15"

    "gender"
    "location"
    "age"
    "recently_displaced"
    "household_language"
    "women_participation"
)

cd "$CODA_V2_ROOT/data_tools"
mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    echo "Getting messages data from ${PROJECT_NAME}_${DATASET}..."

    pipenv run python get.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages >"$DATA_ROOT/Coded Coda Files/$DATASET.json"
done
