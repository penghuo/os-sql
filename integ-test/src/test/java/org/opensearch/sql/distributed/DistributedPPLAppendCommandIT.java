/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLAppendCommandIT;

public class DistributedPPLAppendCommandIT extends CalcitePPLAppendCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
    enableStrictMode();
  }
}
