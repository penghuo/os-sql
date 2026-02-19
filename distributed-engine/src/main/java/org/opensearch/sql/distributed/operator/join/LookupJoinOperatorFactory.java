/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import java.util.Objects;
import java.util.function.Supplier;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;

/**
 * Factory for creating {@link LookupJoinOperator} instances. The factory is initialized with a
 * {@link JoinHash} supplier (typically from the corresponding {@link HashBuilderOperator}).
 *
 * <p>Ported from Trino's io.trino.operator.join.LookupJoinOperatorFactory (simplified).
 */
public class LookupJoinOperatorFactory implements OperatorFactory {

  private final JoinType joinType;
  private final int[] probeKeyChannels;
  private final int[] probeOutputChannels;
  private final int[] buildOutputChannels;
  private final Supplier<JoinHash> joinHashSupplier;
  private boolean closed;

  /**
   * Creates a new LookupJoinOperatorFactory.
   *
   * @param joinType the join type
   * @param probeKeyChannels column indices in probe pages for the join keys
   * @param probeOutputChannels probe-side columns to include in output
   * @param buildOutputChannels build-side columns to include in output
   * @param joinHashSupplier supplier for the build-side JoinHash (from HashBuilderOperator)
   */
  public LookupJoinOperatorFactory(
      JoinType joinType,
      int[] probeKeyChannels,
      int[] probeOutputChannels,
      int[] buildOutputChannels,
      Supplier<JoinHash> joinHashSupplier) {
    this.joinType = Objects.requireNonNull(joinType, "joinType is null");
    this.probeKeyChannels = Objects.requireNonNull(probeKeyChannels, "probeKeyChannels is null");
    this.probeOutputChannels =
        Objects.requireNonNull(probeOutputChannels, "probeOutputChannels is null");
    this.buildOutputChannels =
        Objects.requireNonNull(buildOutputChannels, "buildOutputChannels is null");
    this.joinHashSupplier = Objects.requireNonNull(joinHashSupplier, "joinHashSupplier is null");
  }

  @Override
  public Operator createOperator(OperatorContext operatorContext) {
    if (closed) {
      throw new IllegalStateException("Factory is already closed");
    }
    JoinHash joinHash = joinHashSupplier.get();
    return new LookupJoinOperator(
        operatorContext,
        joinHash,
        joinType,
        probeKeyChannels,
        probeOutputChannels,
        buildOutputChannels);
  }

  @Override
  public void noMoreOperators() {
    closed = true;
  }
}
