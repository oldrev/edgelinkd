#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <Node-RED source directory> <Output JSON directory>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$1"
OUTPUT_DIR="$2"

cd "$TARGET_DIR" || { echo "Directory $TARGET_DIR does not exist."; exit 1; }

OUTPUT_FILE="$OUTPUT_DIR/nodered-nodes-specs.json"

mocha "test/nodes/**/*_spec.js" --dry-run --reporter=json --exit --reporter-options output="$OUTPUT_FILE"

cd - > /dev/null