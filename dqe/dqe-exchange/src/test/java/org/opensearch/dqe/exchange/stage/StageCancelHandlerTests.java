/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.exchange.action.DqeStageCancelRequest;
import org.opensearch.dqe.exchange.action.DqeStageCancelResponse;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

class StageCancelHandlerTests {

  private StageExecutionHandler executionHandler;
  private StageCancelHandler cancelHandler;

  @BeforeEach
  void setUp() {
    executionHandler =
        new StageExecutionHandler(
            mock(ThreadPool.class), mock(TransportService.class), mock(DqeMemoryTracker.class));
    cancelHandler = new StageCancelHandler(executionHandler);
  }

  @Test
  @DisplayName("constructor rejects null executionHandler")
  void constructorRejectsNull() {
    assertThrows(NullPointerException.class, () -> new StageCancelHandler(null));
  }

  @Test
  @DisplayName("messageReceived cancels active stage and sends ACK")
  void messageReceivedCancelsActiveStage() throws Exception {
    // Set up a stage that stays active
    executionHandler.setExecutionCallback((req, onComplete) -> {});
    DqeStageExecuteRequest execReq =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    executionHandler.messageReceived(execReq, new StubTransportChannel(), mock(Task.class));

    AtomicBoolean cancelRan = new AtomicBoolean(false);
    executionHandler.registerCancelAction("q1", 0, () -> cancelRan.set(true));

    assertTrue(executionHandler.isStageActive("q1", 0));

    // Send cancel
    DqeStageCancelRequest cancelReq = new DqeStageCancelRequest("q1", 0);
    StubTransportChannel channel = new StubTransportChannel();
    cancelHandler.messageReceived(cancelReq, channel, mock(Task.class));

    // Cancel action was invoked
    assertTrue(cancelRan.get());

    // ACK was sent
    assertTrue(channel.responseSent);
    DqeStageCancelResponse response = (DqeStageCancelResponse) channel.response;
    assertEquals("q1", response.getQueryId());
    assertEquals(0, response.getStageId());
    assertTrue(response.isAcknowledged());
  }

  @Test
  @DisplayName("messageReceived for unknown stage still sends ACK")
  void messageReceivedForUnknownStageSendsAck() throws Exception {
    DqeStageCancelRequest cancelReq = new DqeStageCancelRequest("unknown", 99);
    StubTransportChannel channel = new StubTransportChannel();
    cancelHandler.messageReceived(cancelReq, channel, mock(Task.class));

    assertTrue(channel.responseSent);
    DqeStageCancelResponse response = (DqeStageCancelResponse) channel.response;
    assertTrue(response.isAcknowledged());
  }

  @Test
  @DisplayName("messageReceived for already-cancelled stage is idempotent")
  void messageReceivedIdempotent() throws Exception {
    executionHandler.setExecutionCallback((req, onComplete) -> {});
    DqeStageExecuteRequest execReq =
        new DqeStageExecuteRequest("q1", 0, new byte[0], Collections.emptyList(), "coord", 1024);
    executionHandler.messageReceived(execReq, new StubTransportChannel(), mock(Task.class));

    // Cancel once
    DqeStageCancelRequest cancelReq = new DqeStageCancelRequest("q1", 0);
    cancelHandler.messageReceived(cancelReq, new StubTransportChannel(), mock(Task.class));

    // Cancel again — should not throw
    StubTransportChannel channel = new StubTransportChannel();
    cancelHandler.messageReceived(cancelReq, channel, mock(Task.class));
    assertTrue(channel.responseSent);
    assertTrue(((DqeStageCancelResponse) channel.response).isAcknowledged());
  }

  /** Minimal stub for TransportChannel that captures the response. */
  private static class StubTransportChannel implements org.opensearch.transport.TransportChannel {
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
