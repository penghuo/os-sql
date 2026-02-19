/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLLookupIT;

public class DistributedPPLLookupIT extends CalcitePPLLookupIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: test indices have pure text fields (no .keyword sub-field),
    // which the distributed engine cannot read via DocValues.
  }
}
