/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

/** Merge Filter --> Filter to the single Filter condition. */
public class MergeLimits implements Rule<LogicalLimit> {

  private final Capture<LogicalLimit> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalLimit> pattern;

  /** Constructor of MergeFilterAndFilter. */
  public MergeLimits() {
    this.capture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalLimit.class)
            .with(source().matching(typeOf(LogicalLimit.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalLimit limit, Captures captures) {
    LogicalLimit childLimit = captures.get(capture);
    if (limit.getOffset() != 0 || childLimit.getOffset() != 0) {
      return limit;
    } else {
      return new LogicalLimit(childLimit.getChild().get(0), Integer.min(limit.getLimit(),
          childLimit.getLimit()), 0);
    }
  }
}
