/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLAggregationPaginatingIT;

public class DistributedPPLAggregationPaginatingIT extends CalcitePPLAggregationPaginatingIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
  }
}
