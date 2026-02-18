/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;

/** Tests that PPL Dedup RelNode is correctly converted to DedupNode. */
class ConvertPPLDedupTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalDedup -> DedupNode with PPL semantics preserved")
  void convertDedup() {
    var scan = createTableScan("test", "name", "age", "city");
    List<RexNode> dedupeFields = List.of(fieldRef(2));
    LogicalDedup dedup = LogicalDedup.create(scan, dedupeFields, 1, false, false);

    PlanNode result = converter.convert(dedup);

    assertInstanceOf(DedupNode.class, result);
    DedupNode dedupNode = (DedupNode) result;
    assertEquals(1, dedupNode.getDedupeFields().size());
    assertEquals(1, dedupNode.getAllowedDuplication());
    assertFalse(dedupNode.isKeepEmpty());
    assertFalse(dedupNode.isConsecutive());
    assertInstanceOf(LuceneTableScanNode.class, dedupNode.getSource());
  }

  @Test
  @DisplayName("LogicalDedup with keepEmpty=true preserves the flag")
  void convertDedupKeepEmpty() {
    var scan = createTableScan("test", "name", "age");
    List<RexNode> dedupeFields = List.of(fieldRef(0));
    LogicalDedup dedup = LogicalDedup.create(scan, dedupeFields, 2, true, false);

    PlanNode result = converter.convert(dedup);

    DedupNode dedupNode = (DedupNode) result;
    assertEquals(2, dedupNode.getAllowedDuplication());
    assertTrue(dedupNode.isKeepEmpty());
  }
}
