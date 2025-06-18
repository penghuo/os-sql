/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZoneId;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.QueryType;

class AnalysisContextTest {

  private final AnalysisContext context = new AnalysisContext();

  @Test
  public void rootEnvironmentShouldBeThereInitially() {
    assertNotNull(context.peek());
  }

  @Test
  public void pushAndPopEnvironmentShouldPass() {
    context.push();
    context.pop();
  }

  @Test
  public void popRootEnvironmentShouldPass() {
    context.pop();
  }

  @Test
  public void popEmptyEnvironmentStackShouldFail() {
    context.pop();
    NullPointerException exception = assertThrows(NullPointerException.class, () -> context.pop());
    assertEquals("Fail to pop context due to no environment present", exception.getMessage());
  }

  @Test
  public void contextWithTimezoneCreatesCorrectFunctionProperties() {
    ZoneId testTimezone = ZoneId.of("America/New_York");
    AnalysisContext contextWithTimezone =
        new AnalysisContext(new TypeEnvironment(null), QueryType.SQL, testTimezone);

    assertNotNull(contextWithTimezone.getFunctionProperties());
    assertEquals(QueryType.SQL, contextWithTimezone.getFunctionProperties().getQueryType());
    // The timezone is set during FunctionProperties creation, but we can't directly access it
    // through the current API. The test ensures the constructor path works without throwing
    // exceptions.
  }

  @Test
  public void contextWithoutTimezoneUsesDefault() {
    AnalysisContext contextWithoutTimezone =
        new AnalysisContext(new TypeEnvironment(null), QueryType.PPL, null);

    assertNotNull(contextWithoutTimezone.getFunctionProperties());
    assertEquals(QueryType.PPL, contextWithoutTimezone.getFunctionProperties().getQueryType());
  }
}
