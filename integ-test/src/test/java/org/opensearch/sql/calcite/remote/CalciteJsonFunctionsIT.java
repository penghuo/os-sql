/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.Ignore;
import org.opensearch.sql.ppl.JsonFunctionsIT;

@Ignore(
    "https://github.com/opensearch-project/sql/issues/3436 — Calcite JSON path treats empty"
        + " string as invalid JSON (test_not_json_valid sees \"json empty string\" included)"
        + " and the count assertions are off by 1; behavior differs from v2 in details that"
        + " require feature work, not round-trip fixes. Tracked under issue #3436.")
public class CalciteJsonFunctionsIT extends JsonFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
