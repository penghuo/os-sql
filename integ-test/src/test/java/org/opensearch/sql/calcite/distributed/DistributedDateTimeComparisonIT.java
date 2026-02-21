/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import org.opensearch.sql.calcite.remote.CalciteDateTimeComparisonIT;

public class DistributedDateTimeComparisonIT extends CalciteDateTimeComparisonIT {

    public DistributedDateTimeComparisonIT(
            String functionCall, String name, Boolean expectedResult) {
        super(functionCall, name, expectedResult);
    }

    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }
}
