#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

#./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="IMAQAL"
DATASETS=(
    #"s01e01"
    #"s01e02"
    #"s01e03"
    #"s01e04"
    #"s01e05"
    #"s01e06"
    #"s01e07"
    "s01mag03"
    "s01mag04"
    "s01mag05"

    "gender"
    "location"
    "age"
    "recently_displaced"
    "household_language"
    "women_participation"
)

cd "$CODA_V2_ROOT/data_tools"

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
