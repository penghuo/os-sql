/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import org.opensearch.sql.calcite.remote.CalciteQueryAnalysisIT;

public class DistributedQueryAnalysisIT extends CalciteQueryAnalysisIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }
}
