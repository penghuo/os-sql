/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * A limit that records when it emits rows and when it reaches its quota.
 *
 * <p>Stands in for the storage module's progress-reporting limit so {@code core} can prove the
 * mechanism — physical callback, substitution, code generation, runtime observation — without
 * depending on a storage engine. Built the same way: delegate {@code implement()} to {@link
 * EnumerableLimit}, then wrap the resulting enumerable.
 */
public final class RecordingLimit extends EnumerableLimit {

  private final List<String> events;

  private RecordingLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RexNode offset,
      RexNode fetch,
      List<String> events) {
    super(cluster, traitSet, input, offset, fetch);
    this.events = events;
  }

  /** Replaces every {@link EnumerableLimit} in {@code plan} with a recording equivalent. */
  public static RelNode wrap(RelNode plan, List<String> events) {
    List<RelNode> inputs = plan.getInputs();
    RelNode current = plan;
    if (!inputs.isEmpty()) {
      List<RelNode> rewritten = new java.util.ArrayList<>(inputs.size());
      boolean changed = false;
      for (RelNode input : inputs) {
        RelNode next = wrap(input, events);
        changed |= next != input;
        rewritten.add(next);
      }
      if (changed) {
        current = current.copy(current.getTraitSet(), rewritten);
      }
    }
    if (current instanceof RecordingLimit) {
      return current;
    }
    if (current instanceof EnumerableLimit limit) {
      return new RecordingLimit(
          limit.getCluster(),
          limit.getTraitSet(),
          limit.getInput(),
          limit.offset,
          limit.fetch,
          events);
    }
    return current;
  }

  @Override
  public RecordingLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new RecordingLimit(
        getCluster(),
        traitSet,
        newInputs.isEmpty() ? getInput() : newInputs.get(0),
        offset,
        fetch,
        events);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref) {
    Result base = super.implement(implementor, pref);
    long quota = fetch instanceof RexLiteral literal ? literal.getValueAs(Long.class) : -1L;
    Expression recorder = implementor.stash(new Recorder(events, quota), Recorder.class);
    BlockBuilder builder = new BlockBuilder();
    Expression limited = builder.append("limited", base.block);
    builder.add(Expressions.return_(null, Expressions.call(recorder, "observe", limited)));
    return implementor.result(base.physType, builder.toBlock());
  }

  /** Stashed into generated code; records each emitted row and the moment the quota is met. */
  public static final class Recorder {

    private final List<String> events;
    private final long quota;

    Recorder(List<String> events, long quota) {
      this.events = events;
      this.quota = quota;
    }

    public <T> Enumerable<T> observe(Enumerable<T> source) {
      return new AbstractEnumerable<T>() {
        @Override
        public Enumerator<T> enumerator() {
          Enumerator<T> delegate = source.enumerator();
          return new Enumerator<T>() {
            private long emitted;
            private boolean signalled;

            @Override
            public T current() {
              return delegate.current();
            }

            @Override
            public boolean moveNext() {
              boolean hasNext = delegate.moveNext();
              if (hasNext) {
                events.add("row");
                if (!signalled && ++emitted >= quota) {
                  signalled = true;
                  events.add("quota-reached");
                }
              }
              return hasNext;
            }

            @Override
            public void reset() {
              delegate.reset();
            }

            @Override
            public void close() {
              delegate.close();
            }
          };
        }
      };
    }
  }
}
