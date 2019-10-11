#!/usr/bin/env bash

set -e

if [[ $# -ne 1 ]]; then
    echo "Usage: ./filter_noise_messages.sh <data-root>"
    echo "A temporary fix to filter out noise messages from '<data-root>/Outputs/Coda Files' before uploading to Coda"
    exit
fi

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
    "s01mag16"
    "s01mag17"
    "s01mag18"
    "s01mag19"
    "s01mag20"
    "s02e08"

    "minority_clan_issues"
    "young_people_issues"
    "decisions_minority_clan"
)

DATA_ROOT=$1

cd ..
for DATASET in ${DATASETS[@]}
do
    echo "Filtering out noise messages in ${DATASET}..."

    pipenv run python filter_noise_messages.py "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
