/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataReader;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataWriter;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public class RemoteSourceOperator extends PhysicalPlan {

  private final QLDataReader dataReader;

  public RemoteSourceOperator(QLDataReader dataReader) {
    this.dataReader = dataReader;
  }

  @Override
  public boolean hasNext() {
    return dataReader.hasNext();
  }

  @Override
  public ExprValue next() {
    return dataReader.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
