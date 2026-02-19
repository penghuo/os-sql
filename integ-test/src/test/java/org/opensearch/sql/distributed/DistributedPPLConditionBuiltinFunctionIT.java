/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLConditionBuiltinFunctionIT;

public class DistributedPPLConditionBuiltinFunctionIT
    extends CalcitePPLConditionBuiltinFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: IS NULL/IS NOT NULL on nested/struct fields not supported in distributed engine
  }
}
