#!/bin/bash

#example: bash set_tag.sh origin v0.0.9

git tag "$1" "$2"
git push "$1" "$2"