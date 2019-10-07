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

    "women_participation"
    "minority_clan_issues"
    "young_people_issues"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout '6e4b5e280b0baded86bc2afbf27f44f542e1aeed'  # (master before we started work on segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done

DATASETS=(
    "s01mag11"
    "s01mag12"
    "s01mag13"
    "s01mag14"
    "s01mag15"
    "s01mag16"
    "s01mag17"
    "s01mag18"
    "s01mag19"
    "s01mag20"

    "gender"
    "location"
    "age"
    "recently_displaced"
    "household_language"
    "decisions_minority_clan"
    )

cd "$CODA_V2_ROOT/data_tools"
git checkout '6019a60c855f5da3d82ad8d1423303ce4d6914b6'  # (master which supports segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
