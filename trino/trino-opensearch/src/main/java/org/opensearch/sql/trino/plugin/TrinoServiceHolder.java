/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.plugin;

import io.trino.execution.SqlTaskManager;
import org.opensearch.sql.trino.bootstrap.TrinoEngine;
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.sql.trino.transport.TrinoJsonCodec;
import org.opensearch.transport.TransportService;

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
  private final TrinoEngine engine;
  private volatile TransportService transportService;

  private TrinoServiceHolder(
      OpenSearchSqlTaskManager taskManager, TrinoJsonCodec codec, TrinoEngine engine) {
    this.taskManager = taskManager;
    this.codec = codec;
    this.engine = engine;
  }

  public static void initialize(OpenSearchSqlTaskManager taskManager, TrinoJsonCodec codec) {
    instance = new TrinoServiceHolder(taskManager, codec, null);
  }

  /**
   * Initialize with the engine's real SqlTaskManager and TrinoEngine reference. This enables
   * cross-node query dispatch: incoming transport requests are handled by the local Trino engine.
   *
   * @param trinoEngine the local TrinoEngine
   */
  public static void initializeWithEngine(TrinoEngine trinoEngine) {
    SqlTaskManager sqlTaskManager = trinoEngine.getCoordinatorTaskManager();
    OpenSearchSqlTaskManager taskManager = new OpenSearchSqlTaskManager(sqlTaskManager);
    TrinoJsonCodec codec = new TrinoJsonCodec();
    instance = new TrinoServiceHolder(taskManager, codec, trinoEngine);
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

  public TrinoEngine getEngine() {
    return engine;
  }

  public TransportService getTransportService() {
    return transportService;
  }

  /** Set the TransportService after the transport layer is available. */
  public static void setTransportService(TransportService ts) {
    if (instance != null) {
      instance.transportService = ts;
    }
  }
}
