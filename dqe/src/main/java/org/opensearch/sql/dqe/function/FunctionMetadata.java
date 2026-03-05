/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import io.trino.spi.type.Type;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.sql.dqe.function.aggregate.AggregateAccumulatorFactory;
import org.opensearch.sql.dqe.function.scalar.ScalarFunctionImplementation;

/** Full descriptor for a registered function including its implementation. */
@Builder
@Getter
public class FunctionMetadata {

  private final String name;
  private final List<Type> argumentTypes;
  private final Type returnType;
  private final FunctionKind kind;
  @Builder.Default private final boolean deterministic = true;
  @Builder.Default private final boolean nullable = true;
  private final ScalarFunctionImplementation scalarImplementation;
  private final AggregateAccumulatorFactory aggregateAccumulatorFactory;
}
