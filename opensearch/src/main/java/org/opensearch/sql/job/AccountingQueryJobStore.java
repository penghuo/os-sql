/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;

/**
 * {@link QueryJobStore} that limits how many finished jobs a delegate store retains for polling.
 *
 * <p>{@link #register(QueryJob)} is the admission point: a job is rejected before it is published
 * or started while {@code plugins.ppl.async.max_retained_jobs} finished jobs are retained. A job
 * {@linkplain #retain(QueryJob) retained} for polling counts against that limit once it finishes.
 * {@link #remove(QueryJobId, QueryJob)} is the single release point for the count, whichever path
 * removes the job: inline completion, DELETE, retention eviction, or {@link #close()}.
 */
public final class AccountingQueryJobStore implements QueryJobStore {

  private final QueryJobStore delegate;
  private final Settings settings;
  // Finished retained jobs, counted against plugins.ppl.async.max_retained_jobs; guarded by this.
  private final Set<QueryJob> finished = Collections.newSetFromMap(new IdentityHashMap<>());

  /**
   * @param delegate store that holds the jobs
   * @param settings plugin settings the limit is read from on every submit
   * @throws NullPointerException if either argument is {@code null}
   */
  public AccountingQueryJobStore(QueryJobStore delegate, Settings settings) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws OpenSearchStatusException with status {@code 429} when the retained jobs are full
   */
  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    int maxRetained = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS);
    if (finished() >= maxRetained) {
      throw new OpenSearchStatusException(
          String.format(
              Locale.ROOT,
              "Async query capacity on this node is exhausted [%s]; retry later",
              Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS.getKeyValue()),
          RestStatus.TOO_MANY_REQUESTS);
    }
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
  public synchronized boolean remove(QueryJobId id, QueryJob job) {
    if (!delegate.remove(id, job)) {
      return false;
    }
    finished.remove(job);
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
