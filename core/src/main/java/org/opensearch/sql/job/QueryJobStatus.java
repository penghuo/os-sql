/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import org.opensearch.sql.executor.progress.QueryProgress;

/**
 * Point-in-time snapshot of a {@link QueryJob}.
 *
 * <p>All fields are immutable and the record enforces the invariants that link the state
 * discriminator to the presence of {@code failure}, {@code result}, and {@code completedAtMillis}.
 * Callers can rely on the discriminator alone; the optional fields are for convenience.
 *
 * @param id opaque job identifier
 * @param state current lifecycle state
 * @param submittedAtMillis wall-clock time the job entered {@link QueryJobState#PENDING}
 * @param startedAtMillis wall-clock time the runner transitioned to {@link QueryJobState#RUNNING}
 * @param completedAtMillis wall-clock time the job reached a terminal state
 * @param failure client-visible failure, present iff {@code state == FAILED}
 * @param result final result, present iff {@code state == SUCCEEDED}
 * @param progress bounded, monotonic completion estimate; exactly {@code 1.0} iff {@code state ==
 *     SUCCEEDED}, and the last observed running value for {@code FAILED} / {@code CANCELLED}
 */
public record QueryJobStatus(
    QueryJobId id,
    QueryJobState state,
    long submittedAtMillis,
    OptionalLong startedAtMillis,
    OptionalLong completedAtMillis,
    Optional<QueryFailure> failure,
    Optional<QueryResult> result,
    QueryProgress progress) {

  /**
   * Validates the snapshot invariants at construction time so that any {@code QueryJobStatus}
   * observed elsewhere is guaranteed self-consistent.
   *
   * <p>Enforces:
   *
   * <ul>
   *   <li>{@code failure} is present iff {@code state == FAILED};
   *   <li>{@code result} is present iff {@code state == SUCCEEDED};
   *   <li>{@code completedAtMillis} is present iff {@code state.isTerminal()};
   *   <li>{@code startedAtMillis} is empty in {@code PENDING};
   *   <li>{@code submittedAtMillis} is non-negative;
   *   <li>{@code progress} reports {@code 1.0} iff {@code state == SUCCEEDED};
   *   <li>no field is {@code null}.
   * </ul>
   *
   * @throws NullPointerException if any argument is {@code null}
   * @throws IllegalArgumentException if any invariant listed above is violated
   */
  public QueryJobStatus {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(state, "state must not be null");
    Objects.requireNonNull(startedAtMillis, "startedAtMillis must not be null");
    Objects.requireNonNull(completedAtMillis, "completedAtMillis must not be null");
    Objects.requireNonNull(failure, "failure must not be null");
    Objects.requireNonNull(result, "result must not be null");
    Objects.requireNonNull(progress, "progress must not be null");
    if (submittedAtMillis < 0) {
      throw new IllegalArgumentException("submittedAtMillis must not be negative");
    }
    // A client must be able to treat fraction_done == 1.0 as "the result is ready" without also
    // reading the status field, so the two can never disagree in either direction.
    if ((progress.fractionDone() == 1.0) != (state == QueryJobState.SUCCEEDED)) {
      throw new IllegalArgumentException(
          "progress 1.0 is only valid for SUCCEEDED state, and SUCCEEDED requires progress 1.0");
    }
    if (failure.isPresent() && state != QueryJobState.FAILED) {
      throw new IllegalArgumentException("failure is only valid for FAILED state");
    }
    if (result.isPresent() && state != QueryJobState.SUCCEEDED) {
      throw new IllegalArgumentException("result is only valid for SUCCEEDED state");
    }
    if (state.isTerminal() && completedAtMillis.isEmpty()) {
      throw new IllegalArgumentException("completedAtMillis is required for terminal states");
    }
    if (state == QueryJobState.PENDING && startedAtMillis.isPresent()) {
      throw new IllegalArgumentException("startedAtMillis must be empty in PENDING state");
    }
  }
}
