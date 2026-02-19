/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLNestedAggregationIT;

public class DistributedPPLNestedAggregationIT extends CalcitePPLNestedAggregationIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: nested field aggregation not supported in distributed DocValues engine
  }
}
