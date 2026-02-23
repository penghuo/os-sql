/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteResponse;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

class StageExecutionHandlerTests {

  private ThreadPool threadPool;
  private TransportService transportService;
  private DqeMemoryTracker memoryTracker;
  private StageExecutionHandler handler;

  @BeforeEach
  void setUp() {
    threadPool = mock(ThreadPool.class);
    transportService = mock(TransportService.class);
    memoryTracker = mock(DqeMemoryTracker.class);
    handler = new StageExecutionHandler(threadPool, transportService, memoryTracker);
  }

  @Test
  @DisplayName("constructor rejects null threadPool")
  void constructorRejectsNullThreadPool() {
    assertThrows(
        NullPointerException.class,
        () -> new StageExecutionHandler(null, transportService, memoryTracker));
  }

  @Test
  @DisplayName("constructor rejects null transportService")
  void constructorRejectsNullTransportService() {
    assertThrows(
        NullPointerException.class,
        () -> new StageExecutionHandler(threadPool, null, memoryTracker));
  }

  @Test
  @DisplayName("constructor rejects null memoryTracker")
  void constructorRejectsNullMemoryTracker() {
    assertThrows(
        NullPointerException.class,
        () -> new StageExecutionHandler(threadPool, transportService, null));
  }

  @Test
  @DisplayName("setExecutionCallback rejects null callback")
  void setExecutionCallbackRejectsNull() {
    assertThrows(NullPointerException.class, () -> handler.setExecutionCallback(null));
  }

  @Test
  @DisplayName("messageReceived tracks active stage and sends ACK")
  void messageReceivedTracksStageAndSendsAck() throws Exception {
    DqeStageExecuteRequest request =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    // ACK was sent
    assertTrue(channel.responseSent);
    DqeStageExecuteResponse response = (DqeStageExecuteResponse) channel.response;
    assertEquals("q1", response.getQueryId());
    assertEquals(0, response.getStageId());
    assertTrue(response.isAccepted());

    // Without callback, stage gets cleaned up immediately
    assertFalse(handler.isStageActive("q1", 0));
  }

  @Test
  @DisplayName("messageReceived with callback executes and tracks stage")
  void messageReceivedWithCallbackExecutes() throws Exception {
    AtomicReference<StageExecutionHandler.StageCompletionListener> capturedListener =
        new AtomicReference<>();
    handler.setExecutionCallback((req, onComplete) -> capturedListener.set(onComplete));

    DqeStageExecuteRequest request =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    // ACK sent
    assertTrue(channel.responseSent);

    // Stage is active while callback hasn't completed
    assertTrue(handler.isStageActive("q1", 0));

    // Complete the callback
    capturedListener.get().onComplete(null);

    // Stage cleaned up
    assertFalse(handler.isStageActive("q1", 0));
  }

  @Test
  @DisplayName("messageReceived with callback handles failure")
  void messageReceivedWithCallbackHandlesFailure() throws Exception {
    AtomicReference<StageExecutionHandler.StageCompletionListener> capturedListener =
        new AtomicReference<>();
    handler.setExecutionCallback((req, onComplete) -> capturedListener.set(onComplete));

    DqeStageExecuteRequest request =
        new DqeStageExecuteRequest("q1", 1, new byte[0], Collections.emptyList(), "coord", 1024);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    assertTrue(handler.isStageActive("q1", 1));

    // Complete with failure
    capturedListener.get().onComplete(new RuntimeException("test failure"));

    // Stage still cleaned up even on failure
    assertFalse(handler.isStageActive("q1", 1));
  }

  @Test
  @DisplayName("cancelQuery cancels all stages for a query")
  void cancelQueryCancelsAllStages() throws Exception {
    AtomicBoolean cancel0 = new AtomicBoolean(false);
    AtomicBoolean cancel1 = new AtomicBoolean(false);

    // Set up callback that keeps stages alive (never calls onComplete)
    handler.setExecutionCallback((req, onComplete) -> {});

    DqeStageExecuteRequest req0 =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    DqeStageExecuteRequest req1 =
        new DqeStageExecuteRequest("q1", 1, new byte[0], Collections.emptyList(), "coord", 1024);

    handler.messageReceived(req0, new StubTransportChannel(), mock(Task.class));
    handler.messageReceived(req1, new StubTransportChannel(), mock(Task.class));

    handler.registerCancelAction("q1", 0, () -> cancel0.set(true));
    handler.registerCancelAction("q1", 1, () -> cancel1.set(true));

    assertTrue(handler.isStageActive("q1", 0));
    assertTrue(handler.isStageActive("q1", 1));

    handler.cancelQuery("q1");

    assertTrue(cancel0.get());
    assertTrue(cancel1.get());
  }

  @Test
  @DisplayName("cancelStage cancels only the specified stage")
  void cancelStageCancelsSpecificStage() throws Exception {
    AtomicBoolean cancel0 = new AtomicBoolean(false);
    AtomicBoolean cancel1 = new AtomicBoolean(false);

    handler.setExecutionCallback((req, onComplete) -> {});

    DqeStageExecuteRequest req0 =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    DqeStageExecuteRequest req1 =
        new DqeStageExecuteRequest("q1", 1, new byte[0], Collections.emptyList(), "coord", 1024);

    handler.messageReceived(req0, new StubTransportChannel(), mock(Task.class));
    handler.messageReceived(req1, new StubTransportChannel(), mock(Task.class));

    handler.registerCancelAction("q1", 0, () -> cancel0.set(true));
    handler.registerCancelAction("q1", 1, () -> cancel1.set(true));

    handler.cancelStage("q1", 0);

    assertTrue(cancel0.get());
    assertFalse(cancel1.get());
    assertFalse(handler.isStageActive("q1", 0));
    assertTrue(handler.isStageActive("q1", 1));
  }

  @Test
  @DisplayName("cancelQuery on unknown query is a no-op")
  void cancelQueryUnknownIsNoOp() {
    // Should not throw
    handler.cancelQuery("nonexistent");
  }

  @Test
  @DisplayName("cancelStage on unknown stage is a no-op")
  void cancelStageUnknownIsNoOp() {
    // Should not throw
    handler.cancelStage("nonexistent", 99);
  }

  @Test
  @DisplayName("isStageActive returns false for unknown query")
  void isStageActiveReturnsFalseForUnknown() {
    assertFalse(handler.isStageActive("unknown", 0));
  }

  @Test
  @DisplayName("getActiveQueryIds returns tracked queries")
  void getActiveQueryIdsReturnsTracked() throws Exception {
    handler.setExecutionCallback((req, onComplete) -> {});

    DqeStageExecuteRequest req =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    handler.messageReceived(req, new StubTransportChannel(), mock(Task.class));

    Set<String> activeIds = handler.getActiveQueryIds();
    assertTrue(activeIds.contains("q1"));
  }

  @Test
  @DisplayName("registerCancelAction and cancelStage invokes the action")
  void registerAndCancelInvokesAction() {
    AtomicBoolean ran = new AtomicBoolean(false);
    handler.registerCancelAction("q1", 0, () -> ran.set(true));
    handler.cancelStage("q1", 0);
    assertTrue(ran.get());
  }

  /** Minimal stub for TransportChannel that captures the response. */
  private static class StubTransportChannel implements TransportChannel {
    boolean responseSent = false;
    Object response;

    @Override
    public String getProfileName() {
      return "stub";
    }

    @Override
    public String getChannelType() {
      return "stub";
    }

    @Override
    public void sendResponse(org.opensearch.core.transport.TransportResponse response)
        throws java.io.IOException {
      this.response = response;
      this.responseSent = true;
    }

    @Override
    public void sendResponse(Exception exception) throws java.io.IOException {
      this.responseSent = true;
    }
  }
}
