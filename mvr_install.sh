#!/bin/sh

MVR_VERSION="${MVR_VERSION:-latest}"
INSTALL_PATH="${MVR_INSTALL:-/usr/local/bin}"

TEMP_DIR=$(mktemp -d)

trap 'rm -rf "$TEMP_DIR"' EXIT

curl -L -o "$TEMP_DIR/mvr.tar.gz" "https://github.com/johanan/mvr/releases/${MVR_VERSION}/download/mvr_Linux_x86_64.tar.gz"
tar -xvf "$TEMP_DIR/mvr.tar.gz" -C "$TEMP_DIR" mvr

mkdir -p "$INSTALL_PATH"

if [ -f "$TEMP_DIR/mvr" ]; then
    if [ "$(id -u)" = 0 ]; then
        install -o root -g root -m 0755 "$TEMP_DIR/mvr" "$INSTALL_PATH/mvr"
    else
        sudo install -o root -g root -m 0755 "$TEMP_DIR/mvr" "$INSTALL_PATH/mvr"
    fi
else
    echo "Error: mvr file not found in the tar archive"
    exit 1
fi