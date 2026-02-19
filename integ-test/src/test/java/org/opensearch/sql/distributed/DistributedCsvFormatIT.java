/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteCsvFormatIT;

public class DistributedCsvFormatIT extends CalciteCsvFormatIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
  }
}
