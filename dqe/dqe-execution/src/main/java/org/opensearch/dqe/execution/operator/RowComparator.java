/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.block.Block;
import java.util.Comparator;
import java.util.List;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.analyzer.sort.SortSpecification.NullOrdering;
import org.opensearch.dqe.analyzer.sort.SortSpecification.SortDirection;

/**
 * Comparator for row tuples (Object arrays) based on SortSpecifications. Used by SortOperator and
 * TopNOperator.
 */
public class RowComparator implements Comparator<Object[]> {

  private final List<SortSpecification> sortSpecs;
  private final List<Integer> sortChannels;

  /**
   * @param sortSpecs the sort specifications
   * @param sortChannels the channel indices in the row array for each sort spec
   */
  public RowComparator(List<SortSpecification> sortSpecs, List<Integer> sortChannels) {
    this.sortSpecs = sortSpecs;
    this.sortChannels = sortChannels;
  }

  @Override
  public int compare(Object[] row1, Object[] row2) {
    for (int i = 0; i < sortSpecs.size(); i++) {
      SortSpecification spec = sortSpecs.get(i);
      int channel = sortChannels.get(i);
      Object val1 = row1[channel];
      Object val2 = row2[channel];

      int cmp = compareNullable(val1, val2, spec.getNullOrdering());
      if (cmp != 0) {
        return spec.getDirection() == SortDirection.DESC ? -cmp : cmp;
      }
    }
    return 0;
  }

  private static int compareNullable(Object a, Object b, NullOrdering nullOrdering) {
    if (a == null && b == null) {
      return 0;
    }
    if (a == null) {
      return nullOrdering == NullOrdering.NULLS_FIRST ? -1 : 1;
    }
    if (b == null) {
      return nullOrdering == NullOrdering.NULLS_FIRST ? 1 : -1;
    }
    return compareNonNull(a, b);
  }

  @SuppressWarnings("unchecked")
  private static int compareNonNull(Object a, Object b) {
    if (a instanceof Number && b instanceof Number) {
      return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
    }
    if (a instanceof String && b instanceof String) {
      return ((String) a).compareTo((String) b);
    }
    if (a instanceof Boolean && b instanceof Boolean) {
      return Boolean.compare((Boolean) a, (Boolean) b);
    }
    if (a instanceof Comparable && b instanceof Comparable) {
      return ((Comparable) a).compareTo(b);
    }
    return a.toString().compareTo(b.toString());
  }

  /** Reads a value from a block at a given position. */
  public static Object readValue(Block block, int pos) {
    if (block.isNull(pos)) {
      return null;
    }
    try {
      return block.getSlice(pos, 0, block.getSliceLength(pos)).toStringUtf8();
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a slice block
    }
    try {
      return block.getLong(pos, 0);
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a long block
    }
    try {
      byte b = block.getByte(pos, 0);
      return b != 0;
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a byte block
    }
    return null;
  }
}
