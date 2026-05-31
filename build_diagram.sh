#!/bin/bash
original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

#go install github.com/davidschlachter/embedded-struct-visualizer@latest
rm -f ./diagram/diagram.dot
rm -f ./diagram/diagram.png
embedded-struct-visualizer -out ./diagram/diagram.dot
dot -Tpng ./diagramdiagram.dot -o ./diagram/diagram.png

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
