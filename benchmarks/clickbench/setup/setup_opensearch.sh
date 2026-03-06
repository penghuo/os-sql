#!/usr/bin/env bash
# setup_opensearch.sh — Download OpenSearch 3.6.0-SNAPSHOT, build SQL plugin, start cluster.
source "$(dirname "$0")/setup_common.sh"

# --- Install JDK 21 ---
install_jdk() {
    if java -version 2>&1 | grep -q '"21'; then
        log "JDK 21 already installed."
        return 0
    fi
    log "Installing JDK 21..."
    if command -v yum &>/dev/null; then
        sudo yum install -y java-21-amazon-corretto-devel
    elif command -v apt-get &>/dev/null; then
        sudo apt-get update
        sudo apt-get install -y openjdk-21-jdk
    else
        die "Cannot install JDK 21 automatically."
    fi
}

# --- Download and extract OpenSearch ---
install_opensearch() {
    if [ -d "$OS_INSTALL_DIR" ]; then
        log "OpenSearch already installed at $OS_INSTALL_DIR"
        return 0
    fi
    log "Downloading OpenSearch 3.6.0-SNAPSHOT..."
    local tarball="/tmp/opensearch-snapshot.tar.gz"
    wget -q --show-progress -O "$tarball" "$OS_SNAPSHOT_URL"

    log "Extracting to $OS_INSTALL_DIR..."
    sudo mkdir -p "$OS_INSTALL_DIR"
    sudo tar -xzf "$tarball" -C "$OS_INSTALL_DIR" --strip-components=1
    rm -f "$tarball"

    # Configure for single-node benchmark (no security)
    cat <<'YAML' | sudo tee "$OS_INSTALL_DIR/config/opensearch.yml" > /dev/null
cluster.name: clickbench
node.name: node-1
network.host: 0.0.0.0
discovery.type: single-node
plugins.security.disabled: true
YAML

    # Set heap to 50% of available memory (capped at 16g)
    local total_mem_kb
    total_mem_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    local heap_mb=$(( total_mem_kb / 1024 / 2 ))
    if [ "$heap_mb" -gt 16384 ]; then heap_mb=16384; fi
    echo "-Xms${heap_mb}m" | sudo tee "$OS_INSTALL_DIR/config/jvm.options.d/heap.options" > /dev/null
    echo "-Xmx${heap_mb}m" | sudo tee -a "$OS_INSTALL_DIR/config/jvm.options.d/heap.options" > /dev/null
    log "OpenSearch heap set to ${heap_mb}m"

    # Create opensearch user if needed
    if ! id opensearch &>/dev/null; then
        sudo useradd -r -s /bin/false opensearch
    fi
    sudo chown -R opensearch:opensearch "$OS_INSTALL_DIR"
}

# --- Build and install SQL plugin ---
install_sql_plugin() {
    local plugin_dir="/tmp/opensearch-sql-plugin"
    local plugin_zip

    # Check if plugin already installed
    if sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" list 2>/dev/null | grep -q opensearch-sql; then
        log "SQL plugin already installed."
        return 0
    fi

    log "Cloning SQL plugin (${SQL_PLUGIN_BRANCH})..."
    rm -rf "$plugin_dir"
    git clone --depth 1 --branch "$SQL_PLUGIN_BRANCH" "$SQL_PLUGIN_REPO" "$plugin_dir"

    log "Building SQL plugin..."
    cd "$plugin_dir"
    ./gradlew assemble -x test -x integTest

    # Find the plugin zip
    plugin_zip=$(find "$plugin_dir/plugin/build/distributions" -name "opensearch-sql-*.zip" | head -1)
    if [ -z "$plugin_zip" ]; then
        die "Plugin ZIP not found after build."
    fi

    log "Installing SQL plugin from $plugin_zip..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install "file://$plugin_zip"

    cd "$BENCH_DIR"
    rm -rf "$plugin_dir"
}

# --- Start OpenSearch ---
start_opensearch() {
    if curl -sf "${OS_URL}" >/dev/null 2>&1; then
        log "OpenSearch already running."
        return 0
    fi

    log "Starting OpenSearch..."
    sudo -u opensearch "$OS_INSTALL_DIR/bin/opensearch" -d -p "$OS_INSTALL_DIR/opensearch.pid"

    wait_for_opensearch 180
}

# --- Create hits index ---
create_index() {
    local mapping_file="$REPO_DIR/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"

    if curl -sf "${OS_URL}/${INDEX_NAME}" >/dev/null 2>&1; then
        log "Index '${INDEX_NAME}' already exists."
        return 0
    fi

    if [ ! -f "$mapping_file" ]; then
        die "Mapping file not found: $mapping_file"
    fi

    log "Creating index '${INDEX_NAME}'..."
    curl -sf -XPUT "${OS_URL}/${INDEX_NAME}" \
        -H 'Content-Type: application/json' \
        -d @"$mapping_file" | jq .

    log "Index '${INDEX_NAME}' created."
}

# --- Main ---
install_jdk
install_opensearch
install_sql_plugin
start_opensearch
create_index

log "OpenSearch setup complete."
curl -sf "${OS_URL}" | jq .
