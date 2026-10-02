/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.executor.progress.CompletionReason;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.progress.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.progress.SourceShapeClassifier;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.tasks.CancellableTask;

/**
 * Supports a simple iteration over a collection for OpenSearch index
 *
 * <p>Analogous to LINQ's System.Collections.Enumerator. Unlike LINQ, if the underlying collection
 * has been modified it is only optional that an implementation of the Enumerator interface detects
 * it and throws a {@link java.util.ConcurrentModificationException}.
 */
public class OpenSearchIndexEnumerator implements Enumerator<Object> {

  /** OpenSearch client. */
  private final OpenSearchClient client;

  private final BackgroundSearchScanner bgScanner;

  private final List<String> fields;

  /** Search request. */
  @EqualsAndHashCode.Include @ToString.Include private OpenSearchRequest request;

  /** Largest number of rows allowed in the response. */
  @EqualsAndHashCode.Include @ToString.Include private final int maxResponseSize;

  /** How many moveNext() calls to perform resource check once. */
  private static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /** ResourceMonitor. */
  private final ResourceMonitor monitor;

  /** Number of rows returned. */
  private Integer queryCount = 0;

  /** Search response for current batch. */
  private Iterator<ExprValue> iterator;

  private ExprValue current = null;

  private CancellableTask cancellableTask;

  /**
   * This scan's progress identity, or {@code null} when the query is not observed. Held for the
   * enumerator's whole life because its work happens on several threads — the worker thread that
   * drives {@code moveNext}, and the background IO thread that fetches pages.
   */
  @Nullable private final ProgressiveQueryContext.Binding progressBinding;

  /**
   * Set once an upstream limit or exhaustion has been reported, so completion is emitted exactly
   * once.
   */
  private boolean progressCompleted;

  /**
   * Set when this scan's own iteration threw, so its close is not read as a normal consumer close.
   */
  private boolean locallyAborted;

  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      int maxResponseSize,
      int maxResultWindow,
      int queryBucketSize,
      OpenSearchRequest request,
      ResourceMonitor monitor) {
    this(client, fields, maxResponseSize, maxResultWindow, queryBucketSize, request, monitor, null);
  }

  /**
   * @param progressBinding progress identity of the physical scan that created this enumerator,
   *     supplied by the scan rather than derived here; {@code null} when the query is not observed
   */
  public OpenSearchIndexEnumerator(
      OpenSearchClient client,
      List<String> fields,
      int maxResponseSize,
      int maxResultWindow,
      int queryBucketSize,
      OpenSearchRequest request,
      ResourceMonitor monitor,
      @Nullable ProgressiveQueryContext.Binding progressBinding) {
    org.opensearch.sql.monitor.ResourceStatus status = monitor.getStatus();
    if (!status.isHealthy()) {
      throw new NonFallbackCalciteException(
          String.format(
              "Insufficient resources to start query: %s. "
                  + "To increase the limit, adjust the 'plugins.query.memory_limit' setting "
                  + "(default: 85%%).",
              status.getFormattedDescription()));
    }

    this.fields = fields;
    this.request = request;
    this.maxResponseSize = maxResponseSize;
    this.monitor = monitor;
    this.client = client;
    this.progressBinding = progressBinding;
    // Shape has to be declared before the first search: afterwards the calculator cannot tell a
    // paged
    // source's "page done" from a single-request source's "source done".
    SourceShapeClassifier.Shape shape = SourceShapeClassifier.classify(request);
    ProgressiveQueryContext.reportShape(progressBinding, shape.unit(), shape.pageSize());
    this.bgScanner =
        new BackgroundSearchScanner(client, maxResultWindow, queryBucketSize, progressBinding);
    this.bgScanner.startScanning(request);
    this.cancellableTask = OpenSearchQueryManager.getCancellableTask();
  }

  private Iterator<ExprValue> fetchNextBatch() {
    BackgroundSearchScanner.SearchBatchResult result = bgScanner.fetchNextBatch(request);
    return result.iterator();
  }

  @Override
  public Object current() {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}
     */
    if (fields.size() == 1) {
      return resolveForCalcite(current, fields.getFirst());
    }
    return fields.stream().map(field -> resolveForCalcite(current, field)).toArray();
  }

  private Object resolveForCalcite(ExprValue value, String rawPath) {
    if (HighlightExpression.HIGHLIGHT_FIELD.equals(rawPath)) {
      ExprValue hl = ExprValueUtils.getTupleValue(value).get(HighlightExpression.HIGHLIGHT_FIELD);
      return (hl != null && !hl.isMissing() && !hl.isNull()) ? hl : null;
    }
    return ExprValueUtils.resolveRefPaths(value, List.of(rawPath.split("\\."))).valueForCalcite();
  }

  @Override
  public boolean moveNext() {
    try {
      return advance();
    } catch (RuntimeException | Error e) {
      // This scan is the thing that failed. Remember it so close() does not read its own teardown
      // as a
      // consumer that stopped on purpose.
      locallyAborted = true;
      throw e;
    }
  }

  private boolean advance() {
    if (queryCount >= maxResponseSize) {
      // The query size limit stopped us, not the index. The source could produce more, but nothing
      // will
      // consume it, so its remaining documents are not part of this query's work.
      reportSourceComplete(CompletionReason.UPSTREAM_LIMIT);
      return false;
    }

    if (cancellableTask != null && cancellableTask.isCancelled()) {
      throw new TaskCancelledException("The task is cancelled.");
    }

    boolean shouldCheck = (queryCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck) {
      org.opensearch.sql.monitor.ResourceStatus status = this.monitor.getStatus();
      if (!status.isHealthy()) {
        throw new NonFallbackCalciteException(
            String.format(
                "Insufficient resources to continue processing query: %s. "
                    + "Rows processed: %d. "
                    + "To increase the limit, adjust the 'plugins.query.memory_limit' setting "
                    + "(default: 85%%).",
                status.getFormattedDescription(), queryCount));
      }
    }

    if (iterator == null || (!iterator.hasNext() && !this.bgScanner.isScanDone())) {
      iterator = fetchNextBatch();
    }
    if (iterator.hasNext()) {
      current = iterator.next();
      queryCount++;
      return true;
    }
    if (bgScanner.isScanDone()) {
      reportSourceComplete(CompletionReason.EXHAUSTED);
    }
    return false;
  }

  /** Reports source completion at most once. */
  private void reportSourceComplete(CompletionReason reason) {
    if (progressCompleted) {
      return;
    }
    progressCompleted = true;
    ProgressiveQueryContext.completeSource(progressBinding, reason);
  }

  /**
   * Whether this close looks like an abort rather than a consumer finishing with the source.
   *
   * <p>Linq4j closes a source inside its own {@code finally}, which runs before the exception
   * reaches the execution engine, so close cannot wait to be told. These are the signals available
   * at that moment: this scan's own failure, the query task being cancelled, the execution thread
   * being interrupted by the query timeout, and a failure the engine already flagged.
   */
  private boolean closingBecauseAborted() {
    return locallyAborted
        || Thread.currentThread().isInterrupted()
        || (cancellableTask != null && cancellableTask.isCancelled())
        || ProgressiveQueryContext.isAborted(progressBinding);
  }

  @Override
  public void reset() {
    bgScanner.reset(request);
    iterator = bgScanner.fetchNextBatch(request).iterator();
    queryCount = 0;
    // Re-enumeration (Calcite driving the inner side of a nested-loop join again) re-opens the
    // source, so
    // completion has to be re-armed; otherwise the replay would be invisible to progress.
    progressCompleted = false;
  }

  @Override
  public void close() {
    // A consumer that closes an unexhausted source has stopped asking on purpose — a `head`, a
    // coordinator-side `take`, a join that found its match. That ends this source's work just as
    // surely as
    // running out of documents, so it has to complete; otherwise a plan whose inner side is capped
    // by `head`
    // would hold the whole query's fraction down for as long as its other sources keep running.
    //
    // The record is provisional rather than an immediate completion: a close during an abort looks
    // identical
    // from here. The execution engine confirms it once draining succeeds, which a failed or
    // cancelled query
    // never reaches.
    if (!progressCompleted && !closingBecauseAborted()) {
      ProgressiveQueryContext.recordConsumerClose(progressBinding);
    }
    iterator = Collections.emptyIterator();
    queryCount = 0;
    bgScanner.close();
    if (request != null) {
      client.forceCleanup(request);
      request = null;
    }
  }
}
