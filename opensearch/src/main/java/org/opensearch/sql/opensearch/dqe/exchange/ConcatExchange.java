/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Concatenates shard results in arbitrary order.
 *
 * <p>Used for simple scans, filters, and eval-only queries where no ordering or aggregation merging
 * is required. This is the cheapest exchange -- no merging logic is applied.
 */
public class ConcatExchange extends Exchange {

  private List<Object[]> mergedRows = Collections.emptyList();

  public ConcatExchange(RelNode shardPlan, RelDataType rowType) {
    super(shardPlan, rowType);
  }

  @Override
  public void setShardResults(List<ShardResult> results) {
    List<Object[]> allRows = new ArrayList<>();
    for (ShardResult result : results) {
      allRows.addAll(result.getRows());
    }
    this.mergedRows = allRows;
  }

  @Override
  public Iterator<Object[]> scan() {
    return mergedRows.iterator();
  }
}
