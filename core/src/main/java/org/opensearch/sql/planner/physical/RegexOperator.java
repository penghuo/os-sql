/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

@ToString
@EqualsAndHashCode(callSuper = false)
public class RegexOperator extends PhysicalPlan {
  private static final Logger log = LogManager.getLogger(RegexOperator.class);
  /**
   * Input Plan.
   */
  @Getter
  private final PhysicalPlan input;
  /**
   * Expression.
   */
  @Getter
  private final Expression expression;
  /**
   * Raw Pattern
   */
  @Getter
  private final String rawPattern;
  /**
   * Pattern.
   */
  @Getter
  private final Pattern pattern;

  @Getter
  private final List<String> groups;

  public RegexOperator(PhysicalPlan input,
                       Expression expression, String pattern, List<String> groups) {
    this.input = input;
    this.expression = expression;
    this.rawPattern = pattern;
    this.pattern = Pattern.compile(rawPattern);
    this.groups = groups;
  }


  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRegex(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();

    ExprValue value = inputValue.bindingTuples().resolve(expression);
    final String s = value.stringValue();

    Matcher matcher = pattern.matcher(s);
    Map<String, ExprValue> exprValueMap = new LinkedHashMap<>();
    if (matcher.matches()) {
      groups.forEach(group -> exprValueMap.put(group, new ExprStringValue(matcher.group(group))));
    } else {
      log.warn("failed to extract pattern {} from input {}", rawPattern, s);
    }
    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}
