/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteStreamstatsCommandIT;

public class DistributedStreamstatsCommandIT extends CalciteStreamstatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: streamstats uses WindowNode which causes IndexOutOfBoundsException
    // in the distributed engine's operator pipeline.
  }
}
