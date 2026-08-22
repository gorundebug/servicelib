#!/bin/bash

#tag param example: github v0.0.10 "comment"

cd "$(dirname "$0")" || { echo "Error: Failed to change directory to script location"; exit 1; }

git add .
git commit -m "$3"
bash set_tag.sh "$1" "$2"