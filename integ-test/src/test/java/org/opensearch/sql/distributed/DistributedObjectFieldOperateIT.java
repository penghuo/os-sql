/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteObjectFieldOperateIT;

public class DistributedObjectFieldOperateIT extends CalciteObjectFieldOperateIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: object fields require nested field traversal not supported by DocValues
  }
}
