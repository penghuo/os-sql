#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Stops a daemon-mode OpenSearch started by start-opensearch.sh --daemon.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="${SCRIPT_DIR}/.opensearch.pid"

if [ ! -f "${PID_FILE}" ]; then
    echo "No PID file found at ${PID_FILE}."
    echo "Trying to find gradle :opensearch-sql-plugin:run process..."
    PIDS=$(pgrep -f "opensearch-sql-plugin:run" 2>/dev/null || true)
    if [ -z "${PIDS}" ]; then
        echo "No running OpenSearch gradle process found."
        exit 0
    fi
    echo "Found PIDs: ${PIDS}"
    kill ${PIDS} 2>/dev/null || true
    echo "Sent SIGTERM."
    exit 0
fi

PID=$(cat "${PID_FILE}")
if kill -0 "${PID}" 2>/dev/null; then
    echo "Stopping OpenSearch (Gradle PID ${PID})..."
    kill "${PID}"
    # Wait for the process tree to exit
    for i in $(seq 1 30); do
        if ! kill -0 "${PID}" 2>/dev/null; then
            echo "Stopped."
            rm -f "${PID_FILE}"
            exit 0
        fi
        sleep 1
    done
    echo "Force killing..."
    kill -9 "${PID}" 2>/dev/null || true
    rm -f "${PID_FILE}"
else
    echo "Process ${PID} not running."
    rm -f "${PID_FILE}"
fi
