/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLRenameIT;

public class DistributedPPLRenameIT extends CalcitePPLRenameIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: STATE_COUNTRY index has 'country' as pure text field (no .keyword sub-field),
    // which the distributed engine cannot read via DocValues.
  }
}
