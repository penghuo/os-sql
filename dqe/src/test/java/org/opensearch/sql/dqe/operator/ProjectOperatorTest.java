/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildMultiColumnPage;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ProjectOperator")
class ProjectOperatorTest {

  @Test
  @DisplayName("Projects specified columns from multi-column page")
  void projectsColumns() {
    // 3 columns, 5 rows: col0 = [0,10,20,30,40], col1 = [1,11,21,31,41], col2 = [2,12,22,32,42]
    Page input = buildMultiColumnPage(3, 5);
    Operator source = new TestPageSource(List.of(input));
    ProjectOperator project = new ProjectOperator(source, List.of(0, 2));

    Page result = project.processNextBatch();
    assertEquals(2, result.getChannelCount());
    assertEquals(5, result.getPositionCount());
    // Verify that column 0 in result is original column 0
    assertEquals(0L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    // Verify that column 1 in result is original column 2
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(1), 0));
  }

  @Test
  @DisplayName("Projects single column")
  void projectsSingleColumn() {
    Page input = buildMultiColumnPage(3, 4);
    Operator source = new TestPageSource(List.of(input));
    ProjectOperator project = new ProjectOperator(source, List.of(1));

    Page result = project.processNextBatch();
    assertEquals(1, result.getChannelCount());
    assertEquals(4, result.getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(11L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    ProjectOperator project = new ProjectOperator(source, List.of(0));

    assertNull(project.processNextBatch());
  }

  @Test
  @DisplayName("Projects across multiple pages")
  void multiplePages() {
    Page page1 = buildMultiColumnPage(2, 3);
    Page page2 = buildMultiColumnPage(2, 2);
    Operator source = new TestPageSource(List.of(page1, page2));
    ProjectOperator project = new ProjectOperator(source, List.of(1));

    Page result1 = project.processNextBatch();
    assertEquals(1, result1.getChannelCount());
    assertEquals(3, result1.getPositionCount());

    Page result2 = project.processNextBatch();
    assertEquals(1, result2.getChannelCount());
    assertEquals(2, result2.getPositionCount());

    assertNull(project.processNextBatch());
  }
}
