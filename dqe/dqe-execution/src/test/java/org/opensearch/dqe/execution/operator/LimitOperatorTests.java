/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.execution.task.DqeQueryCancelledException;
import org.opensearch.dqe.memory.QueryMemoryBudget;

@ExtendWith(MockitoExtension.class)
class LimitOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  private OperatorContext operatorContext;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "LimitOperator", memoryBudget);
  }

  private Page createPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(values.length, builder.build());
  }

  private Operator mockSource(List<Page> pages) {
    Operator source = mock(Operator.class);
    int[] index = {0};
    lenient()
        .when(source.getOutput())
        .thenAnswer(
            inv -> {
              if (index[0] < pages.size()) {
                return pages.get(index[0]++);
              }
              return null;
            });
    lenient().when(source.isFinished()).thenAnswer(inv -> index[0] >= pages.size());
    return source;
  }

  @Test
  @DisplayName("limit returns only requested number of rows")
  void limitReturnsRequestedRows() {
    Operator source = mockSource(List.of(createPage(1, 2, 3, 4, 5)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 3, 0);

    Page result = limit.getOutput();
    assertEquals(3, result.getPositionCount());
    assertTrue(limit.isFinished());
  }

  @Test
  @DisplayName("limit across multiple pages")
  void limitAcrossMultiplePages() {
    Operator source = mockSource(List.of(createPage(1, 2), createPage(3, 4, 5)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 4, 0);

    List<Page> results = new ArrayList<>();
    while (!limit.isFinished()) {
      Page page = limit.getOutput();
      if (page != null) {
        results.add(page);
      }
    }
    int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(4, totalRows);
  }

  @Test
  @DisplayName("offset skips initial rows")
  void offsetSkipsRows() {
    Operator source = mockSource(List.of(createPage(10, 20, 30, 40, 50)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 2, 2);

    Page result = limit.getOutput();
    assertEquals(2, result.getPositionCount());
    // Values should be 30, 40 (skipping first 2)
    assertEquals(30L, result.getBlock(0).getLong(0, 0));
    assertEquals(40L, result.getBlock(0).getLong(1, 0));
  }

  @Test
  @DisplayName("offset larger than page skips entire page")
  void offsetSkipsEntirePage() {
    Operator source = mockSource(List.of(createPage(1, 2), createPage(3, 4, 5)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 2, 3);

    List<Page> results = new ArrayList<>();
    while (!limit.isFinished()) {
      Page page = limit.getOutput();
      if (page != null) {
        results.add(page);
      }
    }
    int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(2, totalRows);
  }

  @Test
  @DisplayName("finished when source exhausted before limit")
  void finishedWhenSourceExhausted() {
    Operator source = mockSource(List.of(createPage(1, 2)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 100, 0);

    Page result = limit.getOutput();
    assertEquals(2, result.getPositionCount());
    // Need another call to detect source finished
    assertNull(limit.getOutput());
    assertTrue(limit.isFinished());
  }

  @Test
  @DisplayName("limit of zero finishes immediately")
  void limitZeroFinishesImmediately() {
    Operator source = mockSource(List.of(createPage(1, 2, 3)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 0, 0);

    Page result = limit.getOutput();
    assertNull(result);
    assertTrue(limit.isFinished());
  }

  @Test
  @DisplayName("checkInterrupted is called on each getOutput")
  void checkInterruptedCalled() {
    Operator source = mockSource(List.of(createPage(1, 2)));
    LimitOperator limit = new LimitOperator(operatorContext, source, 10, 0);

    operatorContext.setInterrupted(true);
    assertThrows(DqeQueryCancelledException.class, limit::getOutput);
  }

  @Test
  @DisplayName("close delegates to source")
  void closeDelegatesToSource() {
    Operator source = mock(Operator.class);
    LimitOperator limit = new LimitOperator(operatorContext, source, 10, 0);
    limit.close();
    org.mockito.Mockito.verify(source).close();
  }
}
