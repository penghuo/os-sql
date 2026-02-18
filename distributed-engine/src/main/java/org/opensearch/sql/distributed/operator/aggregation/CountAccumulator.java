/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import org.opensearch.sql.distributed.data.Block;

/** COUNT(*) and COUNT(column) accumulator. COUNT(*) never returns null, always returns a LONG. */
public class CountAccumulator implements Accumulator {

  private long[] counts;
  private final boolean countAll; // true for COUNT(*), false for COUNT(column)

  public CountAccumulator(boolean countAll) {
    this.countAll = countAll;
    this.counts = new long[64];
  }

  @Override
  public void addInput(int[] groupIds, int groupCount, Block block) {
    ensureCapacity(groupCount);
    for (int i = 0; i < groupIds.length; i++) {
      int groupId = groupIds[i];
      if (countAll || (block != null && !block.isNull(i))) {
        counts[groupId]++;
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    return counts[groupId];
  }

  @Override
  public boolean isNull(int groupId) {
    return false; // COUNT never returns null
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return (long) Long.BYTES * counts.length;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    if (groupCount > counts.length) {
      counts = Arrays.copyOf(counts, Math.max(counts.length * 2, groupCount));
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.LONG;
  }
}
