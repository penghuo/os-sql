/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.CalcitePrometheusDataSourceCommandsIT;

/**
 * Prometheus datasource does not go through DQE (no OpenSearch shards).
 * DQE is intentionally NOT enabled for this test class.
 * Ignored for the same reason as the parent: https://github.com/opensearch-project/sql/issues/3455
 */
@Ignore("https://github.com/opensearch-project/sql/issues/3455")
public class DistributedPrometheusDataSourceCommandsIT extends CalcitePrometheusDataSourceCommandsIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
    }
}
