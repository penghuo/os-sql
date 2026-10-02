/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.Objects;
import org.apache.calcite.rel.RelNode;

/**
 * Lets a caller transform the chosen physical plan immediately before Calcite implements it.
 *
 * <h2>Why this exists</h2>
 *
 * The plan the execution engine hands to the runner is still logical: a {@code head} is a {@code
 * LogicalSort} at that point, and only the planner inside statement preparation turns it into an
 * {@code EnumerableLimit}. Anything that needs to see — or replace — the operators that will
 * actually execute has to act after that conversion. The only earlier opportunity, {@code
 * Hook.PLAN_BEFORE_IMPLEMENTATION}, ignores its handler's return value, so it can observe the
 * physical plan but not change it.
 *
 * <h2>Scope and neutrality</h2>
 *
 * Deliberately knows nothing beyond {@link RelNode}: the transform is supplied by whichever module
 * needs it, so {@code core} gains no dependency on a storage engine. Installation is scoped and
 * thread-bound — statement preparation runs on the thread that installed it, and a pooled thread
 * must not carry a transform into the next query.
 */
public final class PhysicalPlanHook {

  /** Transform applied to the chosen physical plan. Must return an equivalent plan. */
  @FunctionalInterface
  public interface Transform {
    RelNode apply(RelNode physicalPlan);
  }

  /** Undoes one installation. Always use with try-with-resources. */
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  private static final ThreadLocal<Transform> CURRENT = new ThreadLocal<>();

  private PhysicalPlanHook() {
    throw new AssertionError(
        PhysicalPlanHook.class.getCanonicalName()
            + " is a utility class and must not be initialized");
  }

  /** Installs {@code transform} for statement preparation on this thread. */
  public static Scope install(Transform transform) {
    Objects.requireNonNull(transform, "transform must not be null");
    Transform previous = CURRENT.get();
    CURRENT.set(transform);
    return () -> {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    };
  }

  /**
   * Applies the installed transform, or returns {@code physicalPlan} unchanged when none is
   * installed — which is every synchronous query, so their plans are untouched by construction.
   */
  public static RelNode apply(RelNode physicalPlan) {
    Transform transform = CURRENT.get();
    if (transform == null || physicalPlan == null) {
      return physicalPlan;
    }
    RelNode transformed = transform.apply(physicalPlan);
    return transformed == null ? physicalPlan : transformed;
  }
}
