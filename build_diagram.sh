#!/bin/bash
original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

#go install github.com/davidschlachter/embedded-struct-visualizer@latest
rm -f graph.dot
embedded-struct-visualizer -out graph.dot
dot -Tpng graph.dot -o output.png

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
