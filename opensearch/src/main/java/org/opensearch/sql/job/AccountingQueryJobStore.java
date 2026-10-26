/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link QueryJobStore} that enforces {@link QueryJobQuota} around a delegate store.
 *
 * <p>{@link #register(QueryJob)} is the admission point: a job that does not fit is rejected before
 * it is published or started. {@link #remove(QueryJobId, QueryJob)} is the single release point for
 * the stored slot and the retained result charge, whichever path removes the job: inline
 * completion, DELETE, retention eviction, or {@link #close()}.
 */
public final class AccountingQueryJobStore implements QueryJobStore {

  private final QueryJobStore delegate;
  private final QueryJobQuota quota;

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
   * @throws org.opensearch.OpenSearchStatusException with status {@code 429} when the node has no
   *     capacity for another job
   */
  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    quota.admit();
    QueryJob existing;
    try {
      existing = delegate.register(job);
    } catch (RuntimeException e) {
      quota.releaseJob();
      throw e;
    }
    if (existing != null) {
      quota.releaseJob();
    }
    return existing;
  }

  @Override
  public Optional<QueryJob> find(QueryJobId id) {
    return delegate.find(id);
  }

  @Override
  public boolean remove(QueryJobId id, QueryJob job) {
    if (!delegate.remove(id, job)) {
      return false;
    }
    job.detach();
    quota.releaseJob();
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
