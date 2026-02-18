/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * IC-2.6: DSL comparison test. Runs 20 representative queries via both distributed and DSL paths,
 * comparing results row-for-row.
 */
public class ScatterGatherDSLComparisonIT extends ScatterGatherITBase {

  // --- Filter queries ---

  @Test
  @DisplayName("Q1: Simple equality filter")
  public void testQ01SimpleFilter() throws IOException {
    assertDualPathMatch("where status = 200 | fields id, status");
  }

  @Test
  @DisplayName("Q2: Range filter")
  public void testQ02RangeFilter() throws IOException {
    assertDualPathMatch("where age >= 30 AND age < 40 | fields id, age");
  }

  @Test
  @DisplayName("Q3: String equality filter")
  public void testQ03StringFilter() throws IOException {
    assertDualPathMatch("where city = 'Portland' | fields id, city");
  }

  @Test
  @DisplayName("Q4: OR filter")
  public void testQ04OrFilter() throws IOException {
    assertDualPathMatch("where status = 404 OR status = 500 | fields id, status");
  }

  // --- Aggregation queries ---

  @Test
  @DisplayName("Q5: Count by single column")
  public void testQ05CountBy() throws IOException {
    assertDualPathMatch("stats count() by city");
  }

  @Test
  @DisplayName("Q6: Sum by dept")
  public void testQ06SumBy() throws IOException {
    assertDualPathMatch("stats sum(salary) by dept");
  }

  @Test
  @DisplayName("Q7: Avg by city")
  public void testQ07AvgBy() throws IOException {
    assertDualPathMatch("stats avg(age) by city");
  }

  @Test
  @DisplayName("Q8: Min/Max by dept")
  public void testQ08MinMaxBy() throws IOException {
    assertDualPathMatch("stats min(salary), max(salary) by dept");
  }

  @Test
  @DisplayName("Q9: Global count")
  public void testQ09GlobalCount() throws IOException {
    assertDualPathMatch("stats count()");
  }

  @Test
  @DisplayName("Q10: Multiple aggs by city")
  public void testQ10MultiAgg() throws IOException {
    assertDualPathMatch("stats count(), avg(salary), min(age), max(age) by city");
  }

  // --- Sort + Limit queries ---

  @Test
  @DisplayName("Q11: Sort ascending, head 20")
  public void testQ11SortAscHead() throws IOException {
    assertDualPathMatchOrdered("sort age | head 20 | fields id, age");
  }

  @Test
  @DisplayName("Q12: Sort descending, head 50")
  public void testQ12SortDescHead() throws IOException {
    assertDualPathMatchOrdered("sort - salary | head 50 | fields id, salary");
  }

  @Test
  @DisplayName("Q13: Sort by multiple columns")
  public void testQ13SortMulti() throws IOException {
    assertDualPathMatchOrdered("sort city, age | head 30 | fields city, age, id");
  }

  // --- Filter + Aggregation queries ---

  @Test
  @DisplayName("Q14: Filter + count")
  public void testQ14FilterCount() throws IOException {
    assertDualPathMatch("where status = 200 | stats count() by dept");
  }

  @Test
  @DisplayName("Q15: Filter + avg")
  public void testQ15FilterAvg() throws IOException {
    assertDualPathMatch("where age > 25 | stats avg(salary) by dept");
  }

  @Test
  @DisplayName("Q16: Narrow filter + aggregation")
  public void testQ16NarrowFilterAgg() throws IOException {
    assertDualPathMatch("where city = 'NewYork' AND age > 40 | stats count(), avg(salary) by dept");
  }

  // --- Fields projection ---

  @Test
  @DisplayName("Q17: Fields projection")
  public void testQ17FieldsProjection() throws IOException {
    assertDualPathMatch("where status = 301 | fields id, name, city");
  }

  @Test
  @DisplayName("Q18: Head only")
  public void testQ18HeadOnly() throws IOException {
    assertDualPathMatch("head 10 | fields id, status, city");
  }

  // --- Filter + Sort ---

  @Test
  @DisplayName("Q19: Filter + sort + limit")
  public void testQ19FilterSortLimit() throws IOException {
    assertDualPathMatchOrdered("where age > 30 | sort - salary | head 25 | fields id, age, salary");
  }

  @Test
  @DisplayName("Q20: Complex filter + agg")
  public void testQ20ComplexFilterAgg() throws IOException {
    assertDualPathMatch(
        "where status = 200 AND age >= 25 AND age <= 45 | stats count(), avg(salary) by city");
  }

  // --- Helpers ---

  private void assertDualPathMatch(String pplSuffix) throws IOException {
    JSONObject[] results = dualPathQuery(pplSuffix);
    assertResultsMatch(results[0], results[1]);
  }

  private void assertDualPathMatchOrdered(String pplSuffix) throws IOException {
    JSONObject[] results = dualPathQuery(pplSuffix);
    assertResultsMatchOrdered(results[0], results[1]);
  }
}
