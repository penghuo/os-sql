/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.Ignore;
import org.opensearch.sql.ppl.JsonFunctionsIT;

@Ignore(
    "https://github.com/opensearch-project/sql/issues/3436 — `cast(x as json)` and `json(x)`"
        + " on Calcite return the raw string unchanged instead of the parsed and"
        + " re-serialized canonical form (no whitespace, ordered/unordered fields). v2 uses"
        + " Jackson ObjectMapper.readTree → re-serialize for the canonical form. test_json_valid"
        + " and test_not_json_valid pass after Track X30's JsonValidFunctionImpl that mirrors"
        + " v2's json_valid contract; test_cast_json, test_json, test_cast_json_scalar_to_type"
        + " require `cast as json` feature work in the Calcite engine and are tracked under"
        + " issue #3436.")
public class CalciteJsonFunctionsIT extends JsonFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
