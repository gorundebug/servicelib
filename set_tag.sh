#!/bin/bash

#tag param example: v0.0.9

git tag "$1"
git push origin --tags