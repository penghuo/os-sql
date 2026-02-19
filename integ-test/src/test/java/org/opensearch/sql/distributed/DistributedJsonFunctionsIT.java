/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteJsonFunctionsIT;

public class DistributedJsonFunctionsIT extends CalciteJsonFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: JSON functions not supported in distributed engine
  }
}
