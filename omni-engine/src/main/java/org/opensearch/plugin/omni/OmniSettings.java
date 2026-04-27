/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Plugin settings for Omni. Maps to Trino config properties at the OpenSearch boundary.
 * At runtime, {@code ServiceWiring} reads these to construct the Trino service graph.
 */
public final class OmniSettings {

    public static final Setting<Boolean> ENABLED =
            Setting.boolSetting("plugins.omni.enabled", true, Setting.Property.NodeScope);

    // --- Catalog ---
    public static final Setting<String> CATALOG_TYPE =
            Setting.simpleString("plugins.omni.catalog.type", "rest", Setting.Property.NodeScope);

    public static final Setting<String> CATALOG_URI =
            Setting.simpleString("plugins.omni.catalog.uri", "", Setting.Property.NodeScope);

    public static final Setting<String> CATALOG_METASTORE_DIR =
            Setting.simpleString("plugins.omni.catalog.metastore.dir", "", Setting.Property.NodeScope);

    // --- S3 ---
    public static final Setting<String> S3_REGION =
            Setting.simpleString("plugins.omni.s3.region", "", Setting.Property.NodeScope);

    public static final Setting<String> S3_ENDPOINT =
            Setting.simpleString("plugins.omni.s3.endpoint", "", Setting.Property.NodeScope);

    // --- Execution ---
    public static final Setting<String> MAX_QUERY_MEMORY =
            Setting.simpleString("plugins.omni.execution.max_query_memory", "1GB", Setting.Property.NodeScope);

    public static final Setting<String> MAX_TASK_MEMORY =
            Setting.simpleString("plugins.omni.execution.max_task_memory", "256MB", Setting.Property.NodeScope);

    public static final Setting<Integer> MAX_WORKERS_PER_QUERY =
            Setting.intSetting("plugins.omni.execution.max_workers_per_query", 0, 0, Setting.Property.NodeScope);

    public static final Setting<String> EXCHANGE_MAX_BUFFER_SIZE =
            Setting.simpleString("plugins.omni.execution.exchange.max_buffer_size", "32MB", Setting.Property.NodeScope);

    // --- Exchange Transport ---
    public static final Setting<Integer> EXCHANGE_PORT =
            Setting.intSetting("plugins.omni.exchange.port", 9500, 1024, 65535, Setting.Property.NodeScope);

    public static List<Setting<?>> getSettings() {
        return List.of(
                ENABLED,
                CATALOG_TYPE,
                CATALOG_URI,
                CATALOG_METASTORE_DIR,
                S3_REGION,
                S3_ENDPOINT,
                MAX_QUERY_MEMORY,
                MAX_TASK_MEMORY,
                MAX_WORKERS_PER_QUERY,
                EXCHANGE_MAX_BUFFER_SIZE,
                EXCHANGE_PORT
        );
    }

    private OmniSettings() {}
}
