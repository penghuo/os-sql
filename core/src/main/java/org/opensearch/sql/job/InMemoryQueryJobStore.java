/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.job.exceptions.QueryJobCapacityException;

/**
 * Node-local, in-memory {@link QueryJobStore}.
 *
 * <p>The store bounds what the node retains. {@link #register(QueryJob)} refuses a job while the
 * store holds the maximum number of jobs, running or finished, or while retained results fill the
 * result budget. Its jobs charge retained results against the store, and {@link #remove(QueryJobId,
 * QueryJob)} returns a removed job's charge. Limits are read on every check, so a lowered limit
 * applies to the next submit and held jobs are not evicted. Cluster visibility is intentionally not
 * modelled.
 */
public final class InMemoryQueryJobStore implements QueryJobStore {

  private final ConcurrentMap<QueryJobId, QueryJob> jobs = new ConcurrentHashMap<>();
  private final IntSupplier maxJobs;
  private final LongSupplier maxResultBytes;
  private volatile boolean closed;

  // Guarded by this.
  private long resultBytes;

  /** Creates a store without limits. */
  public InMemoryQueryJobStore() {
    this(() -> Integer.MAX_VALUE, () -> Long.MAX_VALUE);
  }

  /**
   * @param maxJobs maximum number of jobs the store holds; read on every {@link #register}
   * @param maxResultBytes budget for the estimated bytes of retained results; read on every check
   * @throws NullPointerException if either argument is {@code null}
   */
  public InMemoryQueryJobStore(IntSupplier maxJobs, LongSupplier maxResultBytes) {
    this.maxJobs = Objects.requireNonNull(maxJobs, "maxJobs must not be null");
    this.maxResultBytes = Objects.requireNonNull(maxResultBytes, "maxResultBytes must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws QueryJobCapacityException when the store holds the maximum number of jobs or retained
   *     results fill the result budget
   */
  @Override
  public QueryJob register(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (closed) {
      throw new IllegalStateException("Query job store is closed");
    }
    int jobLimit = maxJobs.getAsInt();
    long byteLimit = maxResultBytes.getAsLong();
    Settings.Key exhausted;
    synchronized (this) {
      if (jobs.size() >= jobLimit) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS;
      } else if (resultBytes >= byteLimit) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE;
      } else {
        return jobs.putIfAbsent(job.id(), job);
      }
    }
    throw new QueryJobCapacityException(
        String.format(
            Locale.ROOT,
            "Asynchronous query capacity on this node is exhausted [%s]; delete finished queries"
                + " or retry later",
            exhausted.getKeyValue()));
  }

  @Override
  public Optional<QueryJob> find(QueryJobId id) {
    Objects.requireNonNull(id, "id must not be null");
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public boolean remove(QueryJobId id, QueryJob job) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(job, "job must not be null");
    if (!jobs.remove(id, job)) {
      return false;
    }
    job.detach();
    return true;
  }

  @Override
  public Collection<QueryJob> jobs() {
    return List.copyOf(jobs.values());
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (QueryJob job : jobs.values()) {
      job.cancel();
    }
    jobs.clear();
  }

  /**
   * {@inheritDoc}
   *
   * @throws QueryJobCapacityException when {@code bytes} does not fit in the remaining budget
   */
  @Override
  public void reserve(long bytes) {
    long byteLimit = maxResultBytes.getAsLong();
    synchronized (this) {
      if (bytes <= byteLimit - resultBytes) {
        resultBytes += bytes;
        return;
      }
    }
    throw new QueryJobCapacityException(
        String.format(
            Locale.ROOT,
            "The query result does not fit in the retained result budget [%s]; reduce the result"
                + " size, or delete finished queries and retry",
            Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE.getKeyValue()));
  }

  @Override
  public synchronized void release(long bytes) {
    resultBytes -= bytes;
  }

  /** Number of jobs the store holds, running or finished. */
  public int size() {
    return jobs.size();
  }

  /** Estimated bytes of retained results currently charged. */
  public synchronized long resultBytes() {
    return resultBytes;
  }
}
