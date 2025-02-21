#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
SDK_SCRIPT_PATH="${SCRIPT_DIR}/../../flex/interactive/sdk/generate_sdk.sh"
LANGUAGE="spring"

if [ ! -f "$SDK_SCRIPT_PATH" ]; then
  echo "Error: SDK generation script not found at $SDK_SCRIPT_PATH"
  exit 1
fi

echo "Generating Spring files using OpenAPI Generator..."
bash "$SDK_SCRIPT_PATH" -g "$LANGUAGE"

if [ $? -ne 0 ]; then
  echo "Failed to generate Spring files"
  exit 1
else
  echo "Successfully generated Spring files"
fi