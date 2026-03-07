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

    # Configure for single-node benchmark (min distribution has no security plugin)
    cat <<YAML | sudo tee "$OS_INSTALL_DIR/config/opensearch.yml" > /dev/null
cluster.name: clickbench
node.name: node-1
network.host: 0.0.0.0
discovery.type: single-node
path.repo: ["${BENCH_DIR}/snapshots"]
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

# --- Install job-scheduler plugin (required dependency for SQL plugin) ---
install_job_scheduler_plugin() {
    local js_zip

    if sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" list 2>/dev/null | grep -q opensearch-job-scheduler; then
        log "Job-scheduler plugin already installed."
        return 0
    fi

    # Look for cached job-scheduler zip from Gradle
    js_zip=$(find "$HOME/.gradle/caches/modules-2" -name "opensearch-job-scheduler-*.zip" -path "*${OS_VERSION}*" 2>/dev/null | head -1)
    if [ -z "$js_zip" ]; then
        # Trigger download via Gradle dependency resolution
        log "Resolving job-scheduler dependency via Gradle..."
        cd "$REPO_DIR"
        ./gradlew :opensearch-sql-plugin:assemble -x test -x integTest >/dev/null 2>&1
        js_zip=$(find "$HOME/.gradle/caches/modules-2" -name "opensearch-job-scheduler-*.zip" -path "*${OS_VERSION}*" 2>/dev/null | head -1)
    fi

    if [ -z "$js_zip" ]; then
        die "Job-scheduler plugin ZIP not found in Gradle cache."
    fi

    log "Installing job-scheduler plugin from $js_zip..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install --batch "file://$js_zip"
}

# --- Build and install SQL plugin from local repo ---
install_sql_plugin() {
    local plugin_zip

    # Check if plugin already installed
    if sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" list 2>/dev/null | grep -q opensearch-sql; then
        log "SQL plugin already installed."
        return 0
    fi

    log "Building SQL plugin from local repo ($REPO_DIR)..."
    cd "$REPO_DIR"
    ./gradlew :opensearch-sql-plugin:assemble -x test -x integTest

    # Find the plugin zip
    plugin_zip=$(find "$REPO_DIR/plugin/build/distributions" -name "opensearch-sql-*.zip" | head -1)
    if [ -z "$plugin_zip" ]; then
        die "Plugin ZIP not found after build. Check ./gradlew :opensearch-sql-plugin:assemble output."
    fi

    log "Installing SQL plugin from $plugin_zip..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install --batch "file://$plugin_zip"

    cd "$BENCH_DIR"
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
install_job_scheduler_plugin
install_sql_plugin
start_opensearch
create_index

log "OpenSearch setup complete."
curl -sf "${OS_URL}" | jq .
