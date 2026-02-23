/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.analyzer.sort.SortSpecification.NullOrdering;
import org.opensearch.dqe.analyzer.sort.SortSpecification.SortDirection;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.types.DqeTypes;

@ExtendWith(MockitoExtension.class)
class TopNOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  private OperatorContext operatorContext;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "TopNOperator", memoryBudget);
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

  private SortSpecification ascSpec() {
    DqeColumnHandle col = new DqeColumnHandle("val", "val", DqeTypes.BIGINT, true, null, false);
    return new SortSpecification(col, DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
  }

  private List<Long> drainValues(TopNOperator topN) {
    List<Long> allValues = new ArrayList<>();
    while (!topN.isFinished()) {
      Page page = topN.getOutput();
      if (page != null) {
        for (int i = 0; i < page.getPositionCount(); i++) {
          allValues.add(page.getBlock(0).getLong(i, 0));
        }
      }
    }
    return allValues;
  }

  @Test
  @DisplayName("returns null during accumulation phase")
  void returnsNullDuringAccumulation() {
    Operator source = mockSource(List.of(createPage(5, 3, 1, 4, 2)));
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 3, List.of(0));

    // During accumulation, should return null
    Page result = topN.getOutput();
    assertNull(result);
    assertFalse(topN.isFinished());
  }

  @Test
  @DisplayName("returns top N rows sorted ascending from single batch")
  void returnsTopNFromSingleBatch() {
    Operator source = mockSource(List.of(createPage(5, 3, 1, 4, 2)));
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 3, List.of(0));

    List<Long> values = drainValues(topN);
    assertEquals(List.of(1L, 2L, 3L), values);
  }

  @Test
  @DisplayName("CRITICAL: processes ALL input before producing output")
  void processesAllInputBeforeOutput() {
    // The top-3 values across all batches: 1, 2, 3
    // But 1 appears in the last batch - if we didn't process all input, we'd miss it
    Operator source =
        mockSource(List.of(createPage(10, 20, 30), createPage(5, 3), createPage(1, 2)));
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 3, List.of(0));

    List<Long> values = drainValues(topN);
    assertEquals(3, values.size());
    assertEquals(List.of(1L, 2L, 3L), values);
  }

  @Test
  @DisplayName("returns fewer than N when source has fewer rows")
  void returnsFewerWhenSourceSmall() {
    Operator source = mockSource(List.of(createPage(5, 3)));
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 10, List.of(0));

    List<Long> values = drainValues(topN);
    assertEquals(List.of(3L, 5L), values);
  }

  @Test
  @DisplayName("N=1 returns the minimum value")
  void nEqualsOneReturnsMinimum() {
    Operator source = mockSource(List.of(createPage(50, 10, 30, 20, 40)));
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 1, List.of(0));

    List<Long> values = drainValues(topN);
    assertEquals(List.of(10L), values);
  }

  @Test
  @DisplayName("empty source produces no output")
  void emptySourceProducesNoOutput() {
    Operator source = mock(Operator.class);
    lenient().when(source.getOutput()).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(true);

    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 5, List.of(0));

    List<Long> values = drainValues(topN);
    assertTrue(values.isEmpty());
  }

  @Test
  @DisplayName("close clears heap and closes source")
  void closeReleasesResources() {
    Operator source = mock(Operator.class);
    TopNOperator topN =
        new TopNOperator(operatorContext, source, List.of(ascSpec()), 5, List.of(0));
    topN.close();
    org.mockito.Mockito.verify(source).close();
  }

  @Test
  @DisplayName("descending TopN returns largest values")
  void descendingTopN() {
    DqeColumnHandle col = new DqeColumnHandle("val", "val", DqeTypes.BIGINT, true, null, false);
    SortSpecification descSpec =
        new SortSpecification(col, DqeTypes.BIGINT, SortDirection.DESC, NullOrdering.NULLS_LAST);

    Operator source = mockSource(List.of(createPage(1, 5, 3, 2, 4)));
    TopNOperator topN = new TopNOperator(operatorContext, source, List.of(descSpec), 3, List.of(0));

    List<Long> values = drainValues(topN);
    assertEquals(3, values.size());
    assertEquals(List.of(5L, 4L, 3L), values);
  }
}
