/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Scope semantics of the physical-plan callback.
 *
 * <p>Only the thread-binding contract is asserted here. That the callback is invoked after physical
 * conversion, that a replacement it returns is the node which actually executes, and that the
 * replacement observes rows as they are emitted are all established through the production limit by
 * {@code CoordinatorLimitProgressEngineTest}, which runs the real planner, code generator,
 * registrar, and progress signal and is verified against a negative control.
 */
class PhysicalPlanHookTest {

  @Test
  @DisplayName("nothing is installed by default, so a plan passes through untouched")
  void withoutInstallationPlanIsUntouched() {
    RelNode plan = mock(RelNode.class);
    // Every synchronous query takes this path: no transform, no substitution, nothing to go wrong.
    assertSame(plan, PhysicalPlanHook.apply(plan));
  }

  @Test
  @DisplayName("a null plan is tolerated")
  void nullPlanIsTolerated() {
    try (PhysicalPlanHook.Scope scope = PhysicalPlanHook.install(plan -> mock(RelNode.class))) {
      assertSame(null, PhysicalPlanHook.apply(null));
    }
  }

  @Test
  @DisplayName("the installed transform is applied and its result returned")
  void transformIsApplied() {
    RelNode original = mock(RelNode.class);
    RelNode replacement = mock(RelNode.class);
    try (PhysicalPlanHook.Scope scope = PhysicalPlanHook.install(plan -> replacement)) {
      assertSame(replacement, PhysicalPlanHook.apply(original));
    }
  }

  @Test
  @DisplayName("a transform returning null is ignored rather than erasing the plan")
  void nullTransformResultIgnored() {
    RelNode plan = mock(RelNode.class);
    try (PhysicalPlanHook.Scope scope = PhysicalPlanHook.install(p -> null)) {
      assertSame(plan, PhysicalPlanHook.apply(plan));
    }
  }

  @Test
  @DisplayName("a scope restores the previous transform and leaves the thread clean")
  void scopesRestore() {
    RelNode plan = mock(RelNode.class);
    AtomicInteger outer = new AtomicInteger();
    try (PhysicalPlanHook.Scope first =
        PhysicalPlanHook.install(
            p -> {
              outer.incrementAndGet();
              return p;
            })) {
      AtomicInteger inner = new AtomicInteger();
      try (PhysicalPlanHook.Scope second =
          PhysicalPlanHook.install(
              p -> {
                inner.incrementAndGet();
                return p;
              })) {
        PhysicalPlanHook.apply(plan);
        assertEquals(1, inner.get());
        assertEquals(0, outer.get());
      }
      PhysicalPlanHook.apply(plan);
      assertEquals(1, outer.get());
    }
    // Statement preparation runs on a pooled thread, so an installation must not outlive its scope.
    assertSame(plan, PhysicalPlanHook.apply(plan));
  }

  @Test
  @DisplayName("installing a null transform is rejected")
  void nullTransformRejected() {
    assertThrows(NullPointerException.class, () -> PhysicalPlanHook.install(null));
  }
}
