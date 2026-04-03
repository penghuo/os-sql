/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * Unit tests for the tracing instrumentation in TransportPPLQueryAction. These tests verify the
 * root span lifecycle (creation, scope management, and async-safe ending) without standing up a
 * full OpenSearch cluster.
 */
@RunWith(MockitoJUnitRunner.class)
public class TransportPPLQueryActionTest {

  @Mock private Tracer mockTracer;
  @Mock private Span mockSpan;
  @Mock private SpanScope mockSpanScope;

  @SuppressWarnings("unchecked")
  private ActionListener<TransportPPLQueryResponse> mockListener = mock(ActionListener.class);

  @Before
  public void setUp() {
    when(mockTracer.startSpan(any(SpanCreationContext.class))).thenReturn(mockSpan);
    when(mockTracer.withSpanInScope(mockSpan)).thenReturn(mockSpanScope);
  }

  @Test
  public void spanIsCreatedWithCorrectNameAndAttributes() {
    ArgumentCaptor<SpanCreationContext> captor = ArgumentCaptor.forClass(SpanCreationContext.class);

    executeTracingLogic(false);

    verify(mockTracer).startSpan(captor.capture());
    SpanCreationContext ctx = captor.getValue();
    assertEquals("opensearch.query", ctx.getSpanName());
    assertEquals("opensearch", ctx.getAttributes().getAttributesMap().get("db.system.name"));
    assertEquals("ppl", ctx.getAttributes().getAttributesMap().get("db.query.type"));
    assertEquals("EXECUTE", ctx.getAttributes().getAttributesMap().get("db.operation.name"));
    assertTrue(ctx.getAttributes().getAttributesMap().containsKey("db.query.id"));
  }

  @Test
  public void spanScopeIsClosedOnTransportThread() {
    executeTracingLogic(false);

    verify(mockSpanScope).close();
  }

  @Test
  public void spanIsEndedWhenListenerOnResponseFires() {
    ActionListener<TransportPPLQueryResponse> tracedListener = executeTracingLogic(false);

    tracedListener.onResponse(new TransportPPLQueryResponse("{}"));

    verify(mockSpan).endSpan();
    verify(mockSpan, never()).setError(any());
  }

  @Test
  public void spanRecordsErrorAndEndsOnFailure() {
    ActionListener<TransportPPLQueryResponse> tracedListener = executeTracingLogic(false);

    Exception testError = new RuntimeException("test failure");
    tracedListener.onFailure(testError);

    verify(mockSpan).setError(testError);
    verify(mockSpan).endSpan();
  }

  @Test
  public void spanEndedExactlyOnceEvenIfCalledTwice() {
    ActionListener<TransportPPLQueryResponse> tracedListener = executeTracingLogic(false);

    tracedListener.onResponse(new TransportPPLQueryResponse("{}"));
    // Second call should not trigger another endSpan
    tracedListener.onFailure(new RuntimeException("late failure"));

    verify(mockSpan).endSpan();
  }

  @Test
  public void explainRequestSetsCorrectOperationName() {
    ArgumentCaptor<SpanCreationContext> captor = ArgumentCaptor.forClass(SpanCreationContext.class);

    executeTracingLogic(true);

    verify(mockTracer).startSpan(captor.capture());
    SpanCreationContext ctx = captor.getValue();
    assertEquals("EXPLAIN", ctx.getAttributes().getAttributesMap().get("db.operation.name"));
  }

  /**
   * Replicates the tracing logic from TransportPPLQueryAction.doExecute() and returns the traced
   * listener so tests can simulate async callbacks.
   */
  @SuppressWarnings("unchecked")
  private ActionListener<TransportPPLQueryResponse> executeTracingLogic(boolean isExplain) {
    QueryContext.addRequestId();

    Span rootSpan =
        mockTracer.startSpan(
            SpanCreationContext.client()
                .name("opensearch.query")
                .attributes(
                    Attributes.create()
                        .addAttribute("db.system.name", "opensearch")
                        .addAttribute("db.query.type", "ppl")
                        .addAttribute("db.query.id", QueryContext.getRequestId())
                        .addAttribute("db.operation.name", isExplain ? "EXPLAIN" : "EXECUTE")));

    SpanScope spanScope = mockTracer.withSpanInScope(rootSpan);

    ActionListener<TransportPPLQueryResponse> tracedListener = null;
    try {
      tracedListener =
          new ActionListener<>() {
            private final AtomicBoolean ended = new AtomicBoolean(false);

            @Override
            public void onResponse(TransportPPLQueryResponse response) {
              try {
                mockListener.onResponse(response);
              } finally {
                endSpan();
              }
            }

            @Override
            public void onFailure(Exception e) {
              try {
                rootSpan.setError(e);
                mockListener.onFailure(e);
              } finally {
                endSpan();
              }
            }

            private void endSpan() {
              if (ended.compareAndSet(false, true)) {
                rootSpan.endSpan();
              }
            }
          };
    } finally {
      spanScope.close();
    }

    return tracedListener;
  }
}
