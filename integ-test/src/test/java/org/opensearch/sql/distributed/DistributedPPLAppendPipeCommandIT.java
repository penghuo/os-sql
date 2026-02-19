/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLAppendPipeCommandIT;

public class DistributedPPLAppendPipeCommandIT extends CalcitePPLAppendPipeCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
    enableStrictMode();
  }
}
