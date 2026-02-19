/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteDataTypeIT;

public class DistributedDataTypeIT extends CalciteDataTypeIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: test index has geo_point fields which use SORTED_NUMERIC DocValues
    // that the distributed engine does not support yet.
  }
}
