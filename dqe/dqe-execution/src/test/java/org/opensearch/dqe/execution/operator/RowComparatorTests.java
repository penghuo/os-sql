/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.analyzer.sort.SortSpecification.NullOrdering;
import org.opensearch.dqe.analyzer.sort.SortSpecification.SortDirection;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.types.DqeTypes;

class RowComparatorTests {

  private DqeColumnHandle col(String name) {
    return new DqeColumnHandle(name, name, DqeTypes.BIGINT, true, null, false);
  }

  @Test
  @DisplayName("ascending sort orders smaller first")
  void ascendingSortOrdersSmallerFirst() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {10L};
    Object[] row2 = {20L};
    assertTrue(cmp.compare(row1, row2) < 0);
  }

  @Test
  @DisplayName("descending sort orders larger first")
  void descendingSortOrdersLargerFirst() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.DESC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {10L};
    Object[] row2 = {20L};
    assertTrue(cmp.compare(row1, row2) > 0);
  }

  @Test
  @DisplayName("null ordering NULLS_FIRST puts nulls before non-nulls")
  void nullsFirstPutsNullsBefore() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_FIRST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {null};
    Object[] row2 = {10L};
    assertTrue(cmp.compare(row1, row2) < 0);
  }

  @Test
  @DisplayName("null ordering NULLS_LAST puts nulls after non-nulls")
  void nullsLastPutsNullsAfter() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {null};
    Object[] row2 = {10L};
    assertTrue(cmp.compare(row1, row2) > 0);
  }

  @Test
  @DisplayName("both nulls compare as equal")
  void bothNullsEqual() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {null};
    Object[] row2 = {null};
    assertEquals(0, cmp.compare(row1, row2));
  }

  @Test
  @DisplayName("equal values compare as zero")
  void equalValuesCompareZero() {
    SortSpecification spec =
        new SortSpecification(
            col("age"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {42L};
    Object[] row2 = {42L};
    assertEquals(0, cmp.compare(row1, row2));
  }

  @Test
  @DisplayName("string comparison works correctly")
  void stringComparison() {
    DqeColumnHandle nameCol =
        new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, null, false);
    SortSpecification spec =
        new SortSpecification(
            nameCol, DqeTypes.VARCHAR, SortDirection.ASC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec), List.of(0));
    Object[] row1 = {"apple"};
    Object[] row2 = {"banana"};
    assertTrue(cmp.compare(row1, row2) < 0);
  }

  @Test
  @DisplayName("multi-column sort uses secondary key for ties")
  void multiColumnSort() {
    SortSpecification spec1 =
        new SortSpecification(
            col("dept"), DqeTypes.BIGINT, SortDirection.ASC, NullOrdering.NULLS_LAST);
    SortSpecification spec2 =
        new SortSpecification(
            col("salary"), DqeTypes.BIGINT, SortDirection.DESC, NullOrdering.NULLS_LAST);
    RowComparator cmp = new RowComparator(List.of(spec1, spec2), List.of(0, 1));
    Object[] row1 = {1L, 100L};
    Object[] row2 = {1L, 200L};
    // Same dept, salary DESC so 200 should come first (row2 < row1)
    assertTrue(cmp.compare(row1, row2) > 0);
  }

  @Test
  @DisplayName("readValue reads long from BIGINT block")
  void readValueBigint() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(builder, 42L);
    builder.appendNull();
    Block block = builder.build();

    assertEquals(42L, RowComparator.readValue(block, 0));
    assertEquals(null, RowComparator.readValue(block, 1));
  }

  @Test
  @DisplayName("readValue reads string from VARCHAR block")
  void readValueVarchar() {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
    VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice("hello"));
    Block block = builder.build();

    assertEquals("hello", RowComparator.readValue(block, 0));
  }
}
