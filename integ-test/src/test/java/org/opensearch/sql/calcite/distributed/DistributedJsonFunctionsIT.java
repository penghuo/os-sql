/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.CalciteJsonFunctionsIT;

/**
 * DQE mirror for CalciteJsonFunctionsIT. Ignored for the same reason as the parent:
 * https://github.com/opensearch-project/sql/issues/3436
 */
@Ignore("https://github.com/opensearch-project/sql/issues/3436")
public class DistributedJsonFunctionsIT extends CalciteJsonFunctionsIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }
}
