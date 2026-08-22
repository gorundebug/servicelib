#!/bin/bash

#example: bash set_tag.sh origin v0.0.9

cd "$(dirname "$0")" || { echo "Error: Failed to change directory to script location"; exit 1; }

git tag "$2"
git push "$1" "$2"