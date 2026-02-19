/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteFlattenCommandIT;

public class DistributedFlattenCommandIT extends CalciteFlattenCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: flatten/JSON operations not supported in distributed engine
  }
}
