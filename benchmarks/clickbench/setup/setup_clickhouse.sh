#!/usr/bin/env bash
# setup_clickhouse.sh — Install ClickHouse server and client on Amazon Linux / Ubuntu.
source "$(dirname "$0")/setup_common.sh"

install_clickhouse_amzn() {
    log "Installing ClickHouse on Amazon Linux..."
    sudo yum install -y yum-utils
    sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
    sudo yum install -y clickhouse-server clickhouse-client
}

install_clickhouse_ubuntu() {
    log "Installing ClickHouse on Ubuntu/Debian..."
    sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
    curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | \
        sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | \
        sudo tee /etc/apt/sources.list.d/clickhouse.list
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client
}

# Detect distro
if [ -f /etc/os-release ]; then
    . /etc/os-release
    case "$ID" in
        amzn|rhel|centos|fedora)
            install_clickhouse_amzn
            ;;
        ubuntu|debian)
            install_clickhouse_ubuntu
            ;;
        *)
            die "Unsupported distro: $ID. Install ClickHouse manually."
            ;;
    esac
else
    die "Cannot detect OS. Install ClickHouse manually."
fi

# Start ClickHouse
log "Starting ClickHouse server..."
sudo systemctl enable clickhouse-server
sudo systemctl start clickhouse-server

wait_for_clickhouse

log "ClickHouse setup complete."
clickhouse-client --host "$CH_HOST" --port "$CH_PORT" -q "SELECT version()"
