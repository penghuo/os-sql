/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.opensearch.sql.calcite.remote.CalciteGeoPointFormatsIT;

public class DistributedGeoPointFormatsIT extends CalciteGeoPointFormatsIT {
  @Override
  public void init() throws Exception {
    super.init();
    // Disabled: geo_point fields use SORTED_NUMERIC DocValues which the distributed
    // engine does not support yet.
  }
}
