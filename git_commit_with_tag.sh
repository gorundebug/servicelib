#!/bin/bash

#tag param example: v0.0.9 "comment"

git add .
git commit -m "$2"
bash set_tag.sh "$1"