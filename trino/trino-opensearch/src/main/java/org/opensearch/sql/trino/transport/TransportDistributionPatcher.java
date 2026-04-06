/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import com.google.inject.Key;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlQueryExecution;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelectorFactory;
import io.trino.server.testing.TestingTrinoServer;
import java.lang.reflect.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
import org.opensearch.transport.TransportService;

/**
 * Patches the Trino server's Guice-injected components via reflection to use transport-based
 * split-level distribution instead of HTTP.
 *
 * <p>After {@link TestingTrinoServer} is built, this patcher replaces:
 *
 * <ul>
 *   <li>{@code RemoteTaskFactory} → {@link TransportRemoteTaskFactory} (task dispatch via
 *       transport)
 *   <li>{@code InternalNodeManager} in {@code NodeSelectorFactory} → {@link
 *       OpenSearchNodeManager} (cluster topology from OpenSearch)
 * </ul>
 *
 * <p>Data exchange still uses Trino's HTTP server on each node — the {@code
 * DirectExchangeClient} fetches pages from each node's embedded HTTP endpoint.
 */
public final class TransportDistributionPatcher {

  private static final Logger LOG = LogManager.getLogger(TransportDistributionPatcher.class);

  private TransportDistributionPatcher() {}

  /**
   * Patch the server for transport-based split-level distribution.
   *
   * @param server the TestingTrinoServer instance
   * @param transportService OpenSearch transport service for cross-node communication
   * @param nodeManager OpenSearch cluster topology manager
   * @param codec JSON codec for Trino type serialization
   */
  public static void patch(
      TestingTrinoServer server,
      TransportService transportService,
      OpenSearchNodeManager nodeManager,
      TrinoJsonCodec codec) {

    TestingTrinoServer coordinator = server;

    // 1. Get the original HTTP-based RemoteTaskFactory for local delegation
    RemoteTaskFactory originalFactory;
    try {
      SqlQueryExecution.SqlQueryExecutionFactory executionFactory =
          coordinator.getInstance(Key.get(SqlQueryExecution.SqlQueryExecutionFactory.class));
      originalFactory = (RemoteTaskFactory) getField(executionFactory, "remoteTaskFactory");
    } catch (Exception e) {
      LOG.warn("Could not get original RemoteTaskFactory, using null delegate", e);
      originalFactory = null;
    }

    // 2. Replace RemoteTaskFactory with hybrid factory (HTTP for local, transport for remote)
    TransportRemoteTaskFactory transportFactory =
        new TransportRemoteTaskFactory(transportService, nodeManager, codec, originalFactory);
    patchRemoteTaskFactory(coordinator, transportFactory);

    // 3. Replace InternalNodeManager in NodeSelectorFactory (inside NodeScheduler)
    patchNodeManager(coordinator, nodeManager);

    LOG.info(
        "Coordinator patched for transport-based split-level distribution. "
            + "RemoteTaskFactory → TransportRemoteTaskFactory, "
            + "NodeManager → OpenSearchNodeManager");
  }

  private static void patchRemoteTaskFactory(
      TestingTrinoServer coordinator, TransportRemoteTaskFactory factory) {
    try {
      // Get SqlQueryExecutionFactory from Guice injector
      SqlQueryExecution.SqlQueryExecutionFactory executionFactory =
          coordinator.getInstance(
              Key.get(SqlQueryExecution.SqlQueryExecutionFactory.class));

      // Replace remoteTaskFactory field, saving the original for local delegation
      setField(executionFactory, "remoteTaskFactory", factory);
      LOG.info("Patched RemoteTaskFactory → TransportRemoteTaskFactory");
    } catch (Exception e) {
      LOG.error("Failed to patch RemoteTaskFactory", e);
      throw new RuntimeException("Failed to patch coordinator for transport distribution", e);
    }
  }

  private static void patchNodeManager(
      TestingTrinoServer coordinator, OpenSearchNodeManager nodeManager) {
    try {
      // Get NodeScheduler from Guice injector
      NodeScheduler nodeScheduler =
          coordinator.getInstance(Key.get(NodeScheduler.class));

      // Get NodeSelectorFactory from NodeScheduler
      Object nodeSelectorFactory = getField(nodeScheduler, "nodeSelectorFactory");

      // Replace nodeManager field in the NodeSelectorFactory
      setField(nodeSelectorFactory, "nodeManager", nodeManager);
      LOG.info(
          "Patched NodeSelectorFactory.nodeManager → OpenSearchNodeManager ({})",
          nodeSelectorFactory.getClass().getSimpleName());
    } catch (Exception e) {
      LOG.error("Failed to patch InternalNodeManager", e);
      throw new RuntimeException("Failed to patch coordinator node manager", e);
    }
  }

  private static void setField(Object target, String fieldName, Object value) {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            "Cannot set field " + fieldName + " on " + target.getClass().getName(), e);
      }
    }
    throw new RuntimeException(
        "Field " + fieldName + " not found in " + target.getClass().getName()
            + " or its superclasses");
  }

  private static Object getField(Object target, String fieldName) {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            "Cannot get field " + fieldName + " from " + target.getClass().getName(), e);
      }
    }
    throw new RuntimeException(
        "Field " + fieldName + " not found in " + target.getClass().getName()
            + " or its superclasses");
  }
}
