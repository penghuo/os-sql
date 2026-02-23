/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.opensearch.dqe.execution.operator.Operator;

class PipelineTests {

  @Test
  @DisplayName("source returns first operator")
  void sourceReturnsFirstOperator() {
    Operator op1 = mock(Operator.class);
    Operator op2 = mock(Operator.class);
    Pipeline pipeline = new Pipeline(List.of(op1, op2));

    assertSame(op1, pipeline.getSource());
  }

  @Test
  @DisplayName("output returns last operator")
  void outputReturnsLastOperator() {
    Operator op1 = mock(Operator.class);
    Operator op2 = mock(Operator.class);
    Pipeline pipeline = new Pipeline(List.of(op1, op2));

    assertSame(op2, pipeline.getOutput());
  }

  @Test
  @DisplayName("getOperators returns all operators")
  void getOperatorsReturnsAll() {
    Operator op1 = mock(Operator.class);
    Operator op2 = mock(Operator.class);
    Operator op3 = mock(Operator.class);
    Pipeline pipeline = new Pipeline(List.of(op1, op2, op3));

    assertEquals(3, pipeline.getOperators().size());
  }

  @Test
  @DisplayName("operators list is immutable")
  void operatorsListIsImmutable() {
    Pipeline pipeline = new Pipeline(List.of(mock(Operator.class)));
    assertThrows(
        UnsupportedOperationException.class,
        () -> pipeline.getOperators().add(mock(Operator.class)));
  }

  @Test
  @DisplayName("empty operator list throws")
  void emptyOperatorListThrows() {
    assertThrows(IllegalArgumentException.class, () -> new Pipeline(List.of()));
  }

  @Test
  @DisplayName("null operator list throws")
  void nullOperatorListThrows() {
    assertThrows(NullPointerException.class, () -> new Pipeline(null));
  }

  @Test
  @DisplayName("close closes operators in reverse order")
  void closeClosesInReverseOrder() {
    Operator op1 = mock(Operator.class);
    Operator op2 = mock(Operator.class);
    Operator op3 = mock(Operator.class);
    Pipeline pipeline = new Pipeline(List.of(op1, op2, op3));

    pipeline.close();

    InOrder inOrder = inOrder(op3, op2, op1);
    inOrder.verify(op3).close();
    inOrder.verify(op2).close();
    inOrder.verify(op1).close();
  }

  @Test
  @DisplayName("close continues when operator throws")
  void closeContinuesOnException() {
    Operator op1 = mock(Operator.class);
    Operator op2 = mock(Operator.class);
    org.mockito.Mockito.doThrow(new RuntimeException("fail")).when(op2).close();
    Pipeline pipeline = new Pipeline(List.of(op1, op2));

    pipeline.close(); // should not throw

    verify(op1).close();
    verify(op2).close();
  }

  @Test
  @DisplayName("single operator pipeline works")
  void singleOperatorPipeline() {
    Operator op = mock(Operator.class);
    Pipeline pipeline = new Pipeline(List.of(op));

    assertSame(op, pipeline.getSource());
    assertSame(op, pipeline.getOutput());
  }
}
