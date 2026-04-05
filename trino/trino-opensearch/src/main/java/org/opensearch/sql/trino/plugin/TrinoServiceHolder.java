/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.plugin;

import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.sql.trino.transport.TrinoJsonCodec;

/**
 * Holds references to Trino-specific services that are initialized in {@code
 * SQLPlugin.createComponents()} and needed by transport action handlers.
 *
 * <p>Transport actions are instantiated by OpenSearch's Guice injector which doesn't know about our
 * Trino types. This holder bridges the gap: initialized in {@code createComponents()}, accessed by
 * transport handlers via the singleton.
 */
public final class TrinoServiceHolder {

  private static volatile TrinoServiceHolder instance;

  private final OpenSearchSqlTaskManager taskManager;
  private final TrinoJsonCodec codec;

  private TrinoServiceHolder(OpenSearchSqlTaskManager taskManager, TrinoJsonCodec codec) {
    this.taskManager = taskManager;
    this.codec = codec;
  }

  public static void initialize(OpenSearchSqlTaskManager taskManager, TrinoJsonCodec codec) {
    instance = new TrinoServiceHolder(taskManager, codec);
  }

  /**
   * Initialize with default components. Creates OpenSearchSqlTaskManager and TrinoJsonCodec
   * internally to avoid classloader issues (shadow jar relocation).
   */
  public static void initializeDefault() {
    OpenSearchSqlTaskManager taskManager = new OpenSearchSqlTaskManager();
    TrinoJsonCodec codec = new TrinoJsonCodec();
    instance = new TrinoServiceHolder(taskManager, codec);
  }

  public static TrinoServiceHolder getInstance() {
    TrinoServiceHolder h = instance;
    if (h == null) {
      throw new IllegalStateException("TrinoServiceHolder not initialized — Trino disabled?");
    }
    return h;
  }

  public static boolean isInitialized() {
    return instance != null;
  }

  public OpenSearchSqlTaskManager getTaskManager() {
    return taskManager;
  }

  public TrinoJsonCodec getCodec() {
    return codec;
  }
}
