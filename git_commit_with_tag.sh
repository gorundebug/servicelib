#!/bin/bash

#tag param example: github v0.0.10 "comment"

git add .
git commit -m "$3"
bash set_tag.sh "$1" "$2"