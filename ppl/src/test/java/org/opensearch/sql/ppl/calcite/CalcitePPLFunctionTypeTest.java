/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    verifyQueryThrowsException(
        "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name",
        "LOWER function expects {[STRING]}, but got [SHORT]");
  }

  @Test
  public void testComparisonWithDifferentType() {
    verifyQueryThrowsException(
        "source=EMP | where ENAME < 6 | fields ENAME",
        "LESS function expects {[IP,IP],[COMPARABLE_TYPE,COMPARABLE_TYPE]}, but got"
            + " [STRING,INTEGER]");
  }

  @Test
  public void testUnsupportedFunctionsFailFast() {
    verifyQueryUnsupported(
        "source=EMP | eval time_diff = timediff(12, '2009-12-10') | fields time_diff", "timediff");
    verifyQueryUnsupported(
        "source=EMP | eval sub_name = substring(ENAME, 1, 3) | fields sub_name", "substring");
    verifyQueryUnsupported(
        "source=EMP | eval coalesce_name = coalesce(EMPNO, 'Jack', ENAME) | fields coalesce_name",
        "coalesce");
    verifyQueryUnsupported(
        "source=EMP | eval if_name = if(EMPNO > 6, 'Jack', ENAME) | fields if_name", "if");
    verifyQueryUnsupported(
        "source=EMP | eval timestamp = timestamp('2020-08-26 13:49:00', 2009) | fields timestamp",
        "timestamp");
    verifyQueryUnsupported(
        "source=EMP | eval curdate = CURDATE(1) | fields curdate | head 1", "CURDATE");
    verifyQueryUnsupported(
        "source=EMP | where ltrim(EMPNO, DEPTNO) = 'Jim' | fields name, age", "ltrim");
    verifyQueryUnsupported("source=EMP | where reverse(EMPNO) = '3202' | fields year", "reverse");
    verifyQueryUnsupported(
        "source=EMP | where strcmp(10, 'Jane') = 0 | fields name, age", "strcmp");
    verifyQueryUnsupported(
        "source=EMP | head 1 | eval sha256 = SHA2('hello', '256') | fields sha256", "SHA2");
    verifyQueryUnsupported(
        "source=EMP | head 1 | eval sqrt_name = sqrt(ENAME) | fields sqrt_name", "sqrt");
    verifyQueryUnsupported("source=EMP | eval z = mod(0.5, 1, 2) | fields z", "mod");
    verifyQueryUnsupported("source=EMP | eval pi = pi(1) | fields pi", "pi");
    verifyQueryUnsupported("source=EMP | eval log2 = log2(ENAME, JOB) | fields log2", "log2");
    verifyQueryUnsupported(
        "source=EMP | eval formatted = strftime(1521467703, '%Y-%m-%d') | fields formatted",
        "strftime");
  }
}
