/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.execution;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.execution.pit.PitManager;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

class ShardStageExecutorTests {

  @Test
  @DisplayName("constructor rejects null parser")
  void constructorRejectsNullParser() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ShardStageExecutor(
                null,
                mock(DqeAnalyzer.class),
                mock(DqeMetadata.class),
                mock(PitManager.class),
                mock(TransportService.class),
                mock(ClusterService.class),
                mock(DqeMemoryTracker.class),
                mock(ThreadPool.class),
                mock(Client.class)));
  }

  @Test
  @DisplayName("constructor accepts all valid dependencies")
  void constructorAcceptsValidDeps() {
    ShardStageExecutor executor =
        new ShardStageExecutor(
            mock(DqeSqlParser.class),
            mock(DqeAnalyzer.class),
            mock(DqeMetadata.class),
            mock(PitManager.class),
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(DqeMemoryTracker.class),
            mock(ThreadPool.class),
            mock(Client.class));
    assertNotNull(executor);
  }
}
