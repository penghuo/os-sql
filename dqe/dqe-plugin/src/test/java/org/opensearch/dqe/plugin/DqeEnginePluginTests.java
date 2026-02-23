/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.dqe.plugin.routing.EngineRouter;
import org.opensearch.dqe.plugin.settings.DqeSettings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class DqeEnginePluginTests {

  @Mock private ClusterService clusterService;
  @Mock private ThreadPool threadPool;
  @Mock private NodeClient client;

  private DqeEnginePlugin plugin;

  private void setUpPlugin() {
    Set<Setting<?>> knownSettings = new HashSet<>(DqeSettings.getAllSettings());
    ClusterSettings cs = new ClusterSettings(Settings.EMPTY, knownSettings);
    lenient().when(clusterService.getClusterSettings()).thenReturn(cs);
    plugin = new DqeEnginePlugin(Settings.EMPTY, clusterService, threadPool, client);
  }

  @Nested
  @DisplayName("Constructor")
  class ConstructorTests {

    @Test
    @DisplayName("rejects null settings")
    void rejectsNullSettings() {
      assertThrows(
          NullPointerException.class,
          () -> new DqeEnginePlugin(null, clusterService, threadPool, client));
    }

    @Test
    @DisplayName("rejects null clusterService")
    void rejectsNullClusterService() {
      assertThrows(
          NullPointerException.class,
          () -> new DqeEnginePlugin(Settings.EMPTY, null, threadPool, client));
    }

    @Test
    @DisplayName("rejects null threadPool")
    void rejectsNullThreadPool() {
      assertThrows(
          NullPointerException.class,
          () -> new DqeEnginePlugin(Settings.EMPTY, clusterService, null, client));
    }

    @Test
    @DisplayName("rejects null client")
    void rejectsNullClient() {
      assertThrows(
          NullPointerException.class,
          () -> new DqeEnginePlugin(Settings.EMPTY, clusterService, threadPool, null));
    }
  }

  @Nested
  @DisplayName("getExecutorBuilders()")
  class ThreadPoolTests {

    @BeforeEach
    void setUp() {
      setUpPlugin();
    }

    @Test
    @DisplayName("returns exactly 3 thread pool builders")
    void returnsThreeBuilders() {
      List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(Settings.EMPTY);
      assertEquals(3, builders.size());
    }

    @Test
    @DisplayName("thread pool names match expected constants")
    void threadPoolNamesMatchConstants() {
      assertEquals("dqe_worker", DqeEnginePlugin.DQE_WORKER_POOL);
      assertEquals("dqe_exchange", DqeEnginePlugin.DQE_EXCHANGE_POOL);
      assertEquals("dqe_coordinator", DqeEnginePlugin.DQE_COORDINATOR_POOL);
    }

    @Test
    @DisplayName("accepts custom pool sizes from settings")
    void customPoolSizes() {
      Settings customSettings =
          Settings.builder()
              .put("plugins.dqe.worker.pool_size", 8)
              .put("plugins.dqe.exchange.pool_size", 4)
              .put("plugins.dqe.coordinator.pool_size", 3)
              .build();
      List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(customSettings);
      assertEquals(3, builders.size());
    }
  }

  @Nested
  @DisplayName("getSettings()")
  class GetSettingsTests {

    @BeforeEach
    void setUp() {
      setUpPlugin();
    }

    @Test
    @DisplayName("returns 15 settings")
    void returns15Settings() {
      assertEquals(15, plugin.getSettings().size());
    }

    @Test
    @DisplayName("matches DqeSettings.getAllSettings()")
    void matchesDqeSettingsGetAll() {
      assertEquals(DqeSettings.getAllSettings(), plugin.getSettings());
    }
  }

  @Nested
  @DisplayName("getActions()")
  class GetActionsTests {

    @BeforeEach
    void setUp() {
      setUpPlugin();
    }

    @Test
    @DisplayName("returns empty list in Phase 1")
    void returnsEmptyActions() {
      assertTrue(plugin.getActions().isEmpty());
    }
  }

  @Nested
  @DisplayName("Lifecycle")
  class LifecycleTests {

    @BeforeEach
    void setUp() {
      setUpPlugin();
    }

    @Test
    @DisplayName("getEngineRouter() throws before initialize()")
    void getRouterBeforeInit() {
      assertThrows(IllegalStateException.class, () -> plugin.getEngineRouter());
    }

    @Test
    @DisplayName("getEngineRouter() returns non-null after initialize()")
    void getRouterAfterInit() {
      plugin.initialize();
      EngineRouter router = plugin.getEngineRouter();
      assertNotNull(router);
    }

    @Test
    @DisplayName("isEnabled() returns false before initialize()")
    void isEnabledBeforeInit() {
      assertFalse(plugin.isEnabled());
    }

    @Test
    @DisplayName("isEnabled() returns true after initialize() with defaults")
    void isEnabledAfterInit() {
      plugin.initialize();
      assertTrue(plugin.isEnabled());
    }

    @Test
    @DisplayName("close() makes isEnabled() return false")
    void closeDisables() {
      plugin.initialize();
      assertTrue(plugin.isEnabled());
      plugin.close();
      assertFalse(plugin.isEnabled());
    }

    @Test
    @DisplayName("double initialize() is tolerated")
    void doubleInitialize() {
      plugin.initialize();
      assertDoesNotThrow(() -> plugin.initialize());
    }

    @Test
    @DisplayName("getStatsHandler() returns null before PL-8")
    void statsHandlerNull() {
      assertNull(plugin.getStatsHandler());
    }

    @Test
    @DisplayName("getDqeSettings() returns non-null after initialize()")
    void getDqeSettingsAfterInit() {
      plugin.initialize();
      assertNotNull(plugin.getDqeSettings());
    }
  }

  @Nested
  @DisplayName("Engine routing integration")
  class EngineRoutingIntegrationTests {

    @BeforeEach
    void setUp() {
      setUpPlugin();
    }

    @Test
    @DisplayName("router resolves calcite by default")
    void routerDefaultCalcite() {
      plugin.initialize();
      assertEquals("calcite", plugin.getEngineRouter().resolveEngine(null));
    }

    @Test
    @DisplayName("router resolves dqe when request specifies dqe")
    void routerDqeByRequest() {
      plugin.initialize();
      assertEquals("dqe", plugin.getEngineRouter().resolveEngine("dqe"));
    }

    @Test
    @DisplayName("shouldUseDqe returns true when dqe requested and enabled")
    void shouldUseDqeTrue() {
      plugin.initialize();
      assertTrue(plugin.getEngineRouter().shouldUseDqe("dqe"));
    }

    @Test
    @DisplayName("shouldUseDqe returns false for calcite")
    void shouldUseDqeFalseForCalcite() {
      plugin.initialize();
      assertFalse(plugin.getEngineRouter().shouldUseDqe("calcite"));
    }
  }
}
