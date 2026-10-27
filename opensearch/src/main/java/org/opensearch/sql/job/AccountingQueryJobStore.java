/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * {@link QueryJobStore} that enforces {@link QueryJobQuota} around a delegate store.
 *
 * <p>{@link #register(QueryJob)} is the admission point: a job is rejected before it is published
 * or started while the retained jobs or the result budget are full. A job {@linkplain
 * #retain(QueryJob) retained} for polling counts against the retained job limit once it finishes.
 * {@link #remove(QueryJobId, QueryJob)} is the single release point for that count and the retained
 * result charge, whichever path removes the job: inline completion, DELETE, retention eviction, or
 * {@link #close()}.
 */
public final class AccountingQueryJobStore implements QueryJobStore {

  private final QueryJobStore delegate;
  private final QueryJobQuota quota;
  // Finished retained jobs, counted against plugins.ppl.async.max_retained_jobs; guarded by this.
  private final Set<QueryJob> finished = Collections.newSetFromMap(new IdentityHashMap<>());

  /**
   * @param delegate store that holds the jobs
   * @param quota capacity the jobs are admitted against
   * @throws NullPointerException if either argument is {@code null}
   */
  public AccountingQueryJobStore(QueryJobStore delegate, QueryJobQuota quota) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.quota = Objects.requireNonNull(quota, "quota must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws org.opensearch.OpenSearchStatusException with status {@code 429} when the retained jobs
   *     or the result budget are full
   */
  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    quota.checkAdmission(finished());
    return delegate.register(job);
  }

  /** Finished jobs retained for polling. */
  public synchronized int finished() {
    return finished.size();
  }

  @Override
  public void retain(QueryJob job) {
    job.onTerminal(() -> countFinished(job));
  }

  private synchronized void countFinished(QueryJob job) {
    // A job deleted before it finished is no longer in the store and is not counted.
    if (delegate.find(job.id()).orElse(null) == job) {
      finished.add(job);
    }
  }

  @Override
  public Optional<QueryJob> find(QueryJobId id) {
    return delegate.find(id);
  }

  @Override
  public boolean remove(QueryJobId id, QueryJob job) {
    synchronized (this) {
      if (!delegate.remove(id, job)) {
        return false;
      }
      finished.remove(job);
    }
    job.detach();
    return true;
  }

  @Override
  public Collection<QueryJob> jobs() {
    return delegate.jobs();
  }

  @Override
  public void close() {
    for (QueryJob job : delegate.jobs()) {
      if (remove(job.id(), job)) {
        job.cancel();
      }
    }
    delegate.close();
  }
}
