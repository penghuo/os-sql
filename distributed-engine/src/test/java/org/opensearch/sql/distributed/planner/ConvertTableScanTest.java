/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that TableScan is correctly converted to LuceneTableScanNode. */
class ConvertTableScanTest extends ConverterTestBase {

  @Test
  @DisplayName("TableScan -> LuceneTableScanNode with index name preserved")
  void convertTableScan() {
    TableScan scan = createTableScan("my_index", "name", "age", "city");

    PlanNode result = converter.convert(scan);

    assertInstanceOf(LuceneTableScanNode.class, result);
    LuceneTableScanNode scanNode = (LuceneTableScanNode) result;
    assertEquals("my_index", scanNode.getIndexName());
  }

  @Test
  @DisplayName("TableScan projected columns are preserved")
  void convertTableScanColumns() {
    TableScan scan = createTableScan("test", "name", "age", "city");

    PlanNode result = converter.convert(scan);

    LuceneTableScanNode scanNode = (LuceneTableScanNode) result;
    assertEquals(List.of("name", "age", "city"), scanNode.getProjectedColumns());
  }

  @Test
  @DisplayName("TableScan output type is preserved")
  void convertTableScanOutputType() {
    TableScan scan = createTableScan("test", "name", "age");

    PlanNode result = converter.convert(scan);

    LuceneTableScanNode scanNode = (LuceneTableScanNode) result;
    assertNotNull(scanNode.getOutputType());
    assertEquals(2, scanNode.getOutputType().getFieldCount());
  }

  @Test
  @DisplayName("LuceneTableScanNode is a leaf node with no children")
  void tableScanHasNoChildren() {
    TableScan scan = createTableScan("test", "name");

    PlanNode result = converter.convert(scan);

    assertTrue(result.getSources().isEmpty());
  }
}
