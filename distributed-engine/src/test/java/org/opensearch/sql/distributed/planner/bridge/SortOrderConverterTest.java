/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.operator.SortOrder;

class SortOrderConverterTest {

  @Test
  @DisplayName("ASC NULLS FIRST")
  void testAscNullsFirst() {
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST));
    List<SortOrder> orders = SortOrderConverter.convertSortOrders(collation);
    assertEquals(1, orders.size());
    assertEquals(SortOrder.ASC_NULLS_FIRST, orders.get(0));
  }

  @Test
  @DisplayName("ASC NULLS LAST")
  void testAscNullsLast() {
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    assertEquals(SortOrder.ASC_NULLS_LAST, SortOrderConverter.convertSortOrders(collation).get(0));
  }

  @Test
  @DisplayName("DESC NULLS FIRST")
  void testDescNullsFirst() {
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST));
    assertEquals(
        SortOrder.DESC_NULLS_FIRST, SortOrderConverter.convertSortOrders(collation).get(0));
  }

  @Test
  @DisplayName("DESC NULLS LAST")
  void testDescNullsLast() {
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST));
    assertEquals(SortOrder.DESC_NULLS_LAST, SortOrderConverter.convertSortOrders(collation).get(0));
  }

  @Test
  @DisplayName("UNSPECIFIED null direction defaults: ASC→NULLS_LAST, DESC→NULLS_FIRST")
  void testUnspecifiedNullDirection() {
    RelCollation ascUnspecified =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.UNSPECIFIED));
    assertEquals(
        SortOrder.ASC_NULLS_LAST, SortOrderConverter.convertSortOrders(ascUnspecified).get(0));

    RelCollation descUnspecified =
        RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.UNSPECIFIED));
    assertEquals(
        SortOrder.DESC_NULLS_FIRST, SortOrderConverter.convertSortOrders(descUnspecified).get(0));
  }

  @Test
  @DisplayName("Multi-column sort channels")
  void testMultiColumnSortChannels() {
    RelCollation collation =
        RelCollations.of(
            new RelFieldCollation(2, Direction.ASCENDING, NullDirection.LAST),
            new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST));

    List<Integer> channels = SortOrderConverter.extractSortChannels(collation);
    assertEquals(List.of(2, 0), channels);

    List<SortOrder> orders = SortOrderConverter.convertSortOrders(collation);
    assertEquals(SortOrder.ASC_NULLS_LAST, orders.get(0));
    assertEquals(SortOrder.DESC_NULLS_FIRST, orders.get(1));
  }
}
