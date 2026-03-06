#!/usr/bin/env bash
# snapshot.sh — Create and restore EBS snapshots for ClickBench data persistence.
# Usage: ./snapshot.sh create | restore | status
source "$(dirname "$0")/../setup/setup_common.sh"

COMMAND="${1:-status}"

# Get instance and volume metadata via IMDSv2
get_instance_id() {
    local token
    token=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
    curl -sf -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/instance-id"
}

get_az() {
    local token
    token=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
    curl -sf -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/placement/availability-zone"
}

# Find the EBS volume mounted at a given path
get_volume_id() {
    local mount_path="$1"
    local device
    device=$(df "$mount_path" | tail -1 | awk '{print $1}')
    # Map device to EBS volume
    local instance_id
    instance_id=$(get_instance_id)
    aws ec2 describe-volumes \
        --filters "Name=attachment.instance-id,Values=$instance_id" \
        --query "Volumes[].{ID:VolumeId,Device:Attachments[0].Device}" \
        --output text | grep "$device" | awk '{print $1}' || echo ""
}

do_create() {
    log "Creating EBS snapshots..."

    # Flush data to disk
    log "Stopping services for consistent snapshot..."
    sudo systemctl stop clickhouse-server 2>/dev/null || true
    sudo -u opensearch kill "$(cat "$OS_INSTALL_DIR/opensearch.pid" 2>/dev/null)" 2>/dev/null || true
    sleep 5
    sync

    local ch_vol os_vol
    ch_vol=$(get_volume_id "$CH_DATA_DIR")
    os_vol=$(get_volume_id "$OS_DATA_DIR")

    if [ -z "$ch_vol" ] && [ -z "$os_vol" ]; then
        # If both dirs are on the same volume, snapshot once
        local root_vol
        root_vol=$(get_volume_id "/")
        log "Snapshotting root volume $root_vol..."
        local snap_id
        snap_id=$(aws ec2 create-snapshot \
            --volume-id "$root_vol" \
            --description "ClickBench data $(date +%Y-%m-%d)" \
            --query 'SnapshotId' --output text)
        echo "ROOT_SNAPSHOT=$snap_id" > "$SNAPSHOT_CONFIG"
        log "Snapshot created: $snap_id"
    else
        if [ -n "$ch_vol" ]; then
            local ch_snap
            ch_snap=$(aws ec2 create-snapshot \
                --volume-id "$ch_vol" \
                --description "ClickBench ClickHouse data $(date +%Y-%m-%d)" \
                --query 'SnapshotId' --output text)
            echo "CH_SNAPSHOT=$ch_snap" > "$SNAPSHOT_CONFIG"
            log "ClickHouse snapshot: $ch_snap"
        fi
        if [ -n "$os_vol" ]; then
            local os_snap
            os_snap=$(aws ec2 create-snapshot \
                --volume-id "$os_vol" \
                --description "ClickBench OpenSearch data $(date +%Y-%m-%d)" \
                --query 'SnapshotId' --output text)
            echo "OS_SNAPSHOT=$os_snap" >> "$SNAPSHOT_CONFIG"
            log "OpenSearch snapshot: $os_snap"
        fi
    fi

    # Restart services
    sudo systemctl start clickhouse-server
    sudo -u opensearch "$OS_INSTALL_DIR/bin/opensearch" -d -p "$OS_INSTALL_DIR/opensearch.pid"

    log "Snapshot creation complete. Config saved to $SNAPSHOT_CONFIG"
}

do_restore() {
    if [ ! -f "$SNAPSHOT_CONFIG" ]; then
        die "No snapshot config found at $SNAPSHOT_CONFIG. Run 'snapshot.sh create' first."
    fi

    source "$SNAPSHOT_CONFIG"
    local az
    az=$(get_az)

    log "Restoring from snapshots in $az..."

    if [ -n "${ROOT_SNAPSHOT:-}" ]; then
        log "Root snapshot restore: $ROOT_SNAPSHOT"
        log "For root volume snapshots, create a new instance from the snapshot via:"
        log "  aws ec2 create-volume --snapshot-id $ROOT_SNAPSHOT --availability-zone $az --volume-type gp3"
        log "Then attach and mount the volume."
    fi

    if [ -n "${CH_SNAPSHOT:-}" ]; then
        log "Creating volume from ClickHouse snapshot $CH_SNAPSHOT..."
        local ch_vol_id
        ch_vol_id=$(aws ec2 create-volume \
            --snapshot-id "$CH_SNAPSHOT" \
            --availability-zone "$az" \
            --volume-type gp3 \
            --query 'VolumeId' --output text)
        log "Volume $ch_vol_id created. Attach and mount to $CH_DATA_DIR"
    fi

    if [ -n "${OS_SNAPSHOT:-}" ]; then
        log "Creating volume from OpenSearch snapshot $OS_SNAPSHOT..."
        local os_vol_id
        os_vol_id=$(aws ec2 create-volume \
            --snapshot-id "$OS_SNAPSHOT" \
            --availability-zone "$az" \
            --volume-type gp3 \
            --query 'VolumeId' --output text)
        log "Volume $os_vol_id created. Attach and mount to $OS_DATA_DIR"
    fi
}

do_status() {
    if [ ! -f "$SNAPSHOT_CONFIG" ]; then
        log "No snapshots configured."
        exit 0
    fi

    source "$SNAPSHOT_CONFIG"
    log "Snapshot config:"
    cat "$SNAPSHOT_CONFIG"

    for snap_var in ROOT_SNAPSHOT CH_SNAPSHOT OS_SNAPSHOT; do
        snap_id="${!snap_var:-}"
        if [ -n "$snap_id" ]; then
            log "  $snap_var ($snap_id):"
            aws ec2 describe-snapshots --snapshot-ids "$snap_id" \
                --query 'Snapshots[0].{State:State,Progress:Progress,Size:VolumeSize}' \
                --output table 2>/dev/null || log "    (not found or no access)"
        fi
    done
}

case "$COMMAND" in
    create)  do_create ;;
    restore) do_restore ;;
    status)  do_status ;;
    *)       die "Usage: $0 {create|restore|status}" ;;
esac
