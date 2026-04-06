/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.plugin;

import io.trino.execution.SqlTaskManager;
import java.net.URI;
import org.opensearch.sql.trino.bootstrap.TrinoEngine;
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
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
  private volatile OpenSearchNodeManager nodeManager;
  private volatile URI trinoHttpUrl;

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
    SqlTaskManager sqlTaskManager = trinoEngine.getTaskManager();
    OpenSearchSqlTaskManager taskManager = new OpenSearchSqlTaskManager(sqlTaskManager);
    // Get Trino's ObjectMapper from the server's Guice injector — it has all
    // the custom serializers for PlanFragment, SplitAssignment, OutputBuffers, etc.
    com.fasterxml.jackson.databind.ObjectMapper objectMapper;
    try {
      objectMapper = trinoEngine.getServer().getInstance(
          com.google.inject.Key.get(com.fasterxml.jackson.databind.ObjectMapper.class));
    } catch (Exception e) {
      objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    }
    TrinoJsonCodec codec = new TrinoJsonCodec(objectMapper);
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

  public OpenSearchNodeManager getNodeManager() {
    return nodeManager;
  }

  public void setNodeManager(OpenSearchNodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public URI getTrinoHttpUrl() {
    return trinoHttpUrl;
  }

  public void setTrinoHttpUrl(URI trinoHttpUrl) {
    this.trinoHttpUrl = trinoHttpUrl;
  }

  /**
   * Enable split-level distribution on the coordinator. Must be called after both the engine
   * and TransportService are available. Delegates to TrinoEngine.enableSplitLevelDistribution()
   * which sets up the NodeManager and patches the coordinator via reflection.
   *
   * @param clusterService OpenSearch cluster service for node discovery
   */
  public void enableSplitLevelDistribution(
      org.opensearch.cluster.service.ClusterService clusterService) {
    if (engine == null || transportService == null) {
      return;
    }
    engine.enableSplitLevelDistribution(clusterService, transportService);
  }
}
