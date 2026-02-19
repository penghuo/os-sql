/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLGrokIT;

public class DistributedPPLGrokIT extends CalcitePPLGrokIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: grok operations parse raw text fields not available via DocValues
  }
}
