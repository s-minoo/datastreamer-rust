#!/usr/bin/env bash

function divider() {
    local char=$1
    if [[ -z "${1+x}" ]]; then
       char="=" 
    fi
    echo "" 
    printf "${char}%.0s"  $(seq 1 63) 
    echo "" 
}

BASE_PATH="/usr/src/data-streamer"
CONFIG_FILE="config.toml"
mkdir -p log
echo "Starting data streamer"
docker-compose up -d
divider
