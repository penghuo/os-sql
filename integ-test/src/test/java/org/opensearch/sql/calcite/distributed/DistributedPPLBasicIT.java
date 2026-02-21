/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import org.opensearch.sql.calcite.remote.CalcitePPLBasicIT;

public class DistributedPPLBasicIT extends CalcitePPLBasicIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }
}
