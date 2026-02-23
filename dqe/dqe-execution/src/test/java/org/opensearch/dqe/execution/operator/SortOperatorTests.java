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
class SortOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  private OperatorContext operatorContext;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "SortOperator", memoryBudget);
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

  private SortSpecification descSpec() {
    DqeColumnHandle col = new DqeColumnHandle("val", "val", DqeTypes.BIGINT, true, null, false);
    return new SortSpecification(col, DqeTypes.BIGINT, SortDirection.DESC, NullOrdering.NULLS_LAST);
  }

  @Test
  @DisplayName("returns null during accumulation phase")
  void returnsNullDuringAccumulation() {
    Operator source = mockSource(List.of(createPage(3, 1, 2)));
    SortOperator sort = new SortOperator(operatorContext, source, List.of(ascSpec()), List.of(0));

    // First call: accumulates but source not finished yet (there's still data)
    // Actually source has 1 page, after reading it source.isFinished() returns true
    // But during accumulation, getOutput returns null
    Page result = sort.getOutput();
    assertNull(result);
  }

  @Test
  @DisplayName("accumulates ALL input before producing output")
  void accumulatesAllInput() {
    Operator source = mockSource(List.of(createPage(5, 3), createPage(1, 4, 2)));
    SortOperator sort = new SortOperator(operatorContext, source, List.of(ascSpec()), List.of(0));

    // Accumulation phase
    assertNull(sort.getOutput()); // reads first page, source not done
    assertNull(sort.getOutput()); // reads second page, source done but just accumulated
    assertFalse(sort.isFinished());

    // Output phase
    List<Long> allValues = new ArrayList<>();
    while (!sort.isFinished()) {
      Page page = sort.getOutput();
      if (page != null) {
        for (int i = 0; i < page.getPositionCount(); i++) {
          allValues.add(page.getBlock(0).getLong(i, 0));
        }
      }
    }

    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), allValues);
  }

  @Test
  @DisplayName("descending sort produces reverse order")
  void descendingSortProducesReverseOrder() {
    Operator source = mockSource(List.of(createPage(1, 3, 2)));
    SortOperator sort = new SortOperator(operatorContext, source, List.of(descSpec()), List.of(0));

    // Drain accumulation
    while (!sort.isFinished()) {
      Page page = sort.getOutput();
      if (page == null && !sort.isFinished()) {
        continue;
      }
      if (page != null) {
        // Verify descending
        assertEquals(3L, page.getBlock(0).getLong(0, 0));
        assertEquals(2L, page.getBlock(0).getLong(1, 0));
        assertEquals(1L, page.getBlock(0).getLong(2, 0));
        break;
      }
    }
  }

  @Test
  @DisplayName("empty source produces no output")
  void emptySourceProducesNoOutput() {
    Operator source = mock(Operator.class);
    lenient().when(source.getOutput()).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(true);

    SortOperator sort = new SortOperator(operatorContext, source, List.of(ascSpec()), List.of(0));

    // After reading null and source is finished, should go to output phase
    assertNull(sort.getOutput());
    // Now output phase with no data
    assertNull(sort.getOutput());
    assertTrue(sort.isFinished());
  }

  @Test
  @DisplayName("close clears accumulated rows and closes source")
  void closeReleasesResources() {
    Operator source = mock(Operator.class);
    SortOperator sort = new SortOperator(operatorContext, source, List.of(ascSpec()), List.of(0));
    sort.close();
    org.mockito.Mockito.verify(source).close();
  }
}
