/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPage;
import static org.opensearch.sql.dqe.operator.TestPageSource.drainOperator;

import io.trino.spi.Page;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LimitOperator")
class LimitOperatorTest {

  @Test
  @DisplayName("Limits output to specified number of rows across pages")
  void limitsRows() {
    Operator source = new TestPageSource(List.of(buildBigintPage(5), buildBigintPage(5)));
    LimitOperator limit = new LimitOperator(source, 7);

    List<Page> pages = drainOperator(limit);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(7, totalRows);
  }

  @Test
  @DisplayName("Returns null after limit reached")
  void returnsNullAfterLimit() {
    Operator source = new TestPageSource(List.of(buildBigintPage(5)));
    LimitOperator limit = new LimitOperator(source, 3);

    Page first = limit.processNextBatch();
    assertNotNull(first);
    assertEquals(3, first.getPositionCount());
    assertNull(limit.processNextBatch());
  }

  @Test
  @DisplayName("Passes through when source has fewer rows than limit")
  void passthroughWhenUnderLimit() {
    Operator source = new TestPageSource(List.of(buildBigintPage(3)));
    LimitOperator limit = new LimitOperator(source, 100);

    List<Page> pages = drainOperator(limit);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(3, totalRows);
  }

  @Test
  @DisplayName("Handles exact boundary where limit equals total rows")
  void exactBoundary() {
    Operator source = new TestPageSource(List.of(buildBigintPage(5)));
    LimitOperator limit = new LimitOperator(source, 5);

    List<Page> pages = drainOperator(limit);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    LimitOperator limit = new LimitOperator(source, 10);

    assertNull(limit.processNextBatch());
  }
}
