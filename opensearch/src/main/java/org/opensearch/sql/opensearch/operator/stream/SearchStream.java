/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@EqualsAndHashCode
@ToString
public class SearchStream extends PhysicalPlan {

  private Iterator<ExprValue> iterator;

  public SearchStream() {

  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitSearchStream(this, context);
  }

  @Override
  public void open() {
    super.open();

    int N = 5;
    int base = 0;
    if (new Random().nextBoolean()) {
      base = 1;
    }
    List<ExprValue> mockTuples = new ArrayList<>();
    for (int i = 0; i < N; i++) {
      mockTuples.add(ExprValueUtils.tupleValue(ImmutableMap.of(String.valueOf(0), base)));
    }
    iterator = mockTuples.listIterator();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of();
  }
}
