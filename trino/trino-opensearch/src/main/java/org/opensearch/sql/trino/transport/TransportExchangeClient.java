/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

/**
 * Merges pages from multiple upstream {@link TransportPageBufferClient}s. Replaces Trino's
 * HTTP-based ExchangeClient.
 *
 * <p>Used by exchange operators in downstream stages to pull data from upstream stages running on
 * (potentially remote) nodes.
 */
public class TransportExchangeClient implements Closeable {

  private static final Logger LOG = LogManager.getLogger(TransportExchangeClient.class);
  private static final long DEFAULT_MAX_BYTES = 32 * 1024 * 1024; // 32MB

  private final TransportService transportService;
  private final List<TransportPageBufferClient> sources = new CopyOnWriteArrayList<>();
  private final LinkedBlockingQueue<byte[]> pageQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger completedSources = new AtomicInteger(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public TransportExchangeClient(TransportService transportService) {
    this.transportService = transportService;
  }

  /**
   * Add a source (upstream node + task + buffer). Called by the scheduler when it knows which nodes
   * are running upstream stages.
   */
  public void addSource(DiscoveryNode node, String taskId, int bufferId) {
    TransportPageBufferClient client =
        new TransportPageBufferClient(transportService, node, taskId, bufferId);
    sources.add(client);
    scheduleFetch(client);
  }

  /**
   * Get next page. Blocks until a page is available or all sources are complete. Returns null when
   * all sources are exhausted.
   *
   * @param timeoutMs maximum time to wait in milliseconds
   * @return page bytes, or null if all sources are complete
   */
  public byte[] getNextPage(long timeoutMs) throws InterruptedException {
    while (!closed.get()) {
      byte[] page = pageQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (page != null) {
        return page;
      }
      // Check if all sources are done
      if (completedSources.get() >= sources.size() && pageQueue.isEmpty()) {
        return null;
      }
    }
    return null;
  }

  /** Check if all sources have completed and all pages have been consumed. */
  public boolean isFinished() {
    return completedSources.get() >= sources.size() && pageQueue.isEmpty();
  }

  private void scheduleFetch(TransportPageBufferClient client) {
    if (closed.get()) {
      return;
    }
    client.getPages(
        DEFAULT_MAX_BYTES,
        new ActionListener<>() {
          @Override
          public void onResponse(TrinoTaskResultsResponse response) {
            // Enqueue pages
            byte[] pages = response.getPages();
            if (pages != null && pages.length > 0) {
              pageQueue.offer(pages);
            }

            if (response.isBufferComplete()) {
              completedSources.incrementAndGet();
              LOG.debug(
                  "Source completed: task={}, buffer={}, completed={}/{}",
                  client.getTaskId(),
                  client.getBufferId(),
                  completedSources.get(),
                  sources.size());
            } else {
              // More pages available — schedule next fetch
              scheduleFetch(client);
            }
          }

          @Override
          public void onFailure(Exception e) {
            LOG.error(
                "Failed to fetch pages from task={}, buffer={}: {}",
                client.getTaskId(),
                client.getBufferId(),
                e.getMessage());
            completedSources.incrementAndGet();
          }
        });
  }

  @Override
  public void close() {
    closed.set(true);
    for (TransportPageBufferClient client : sources) {
      client.close();
    }
    pageQueue.clear();
  }

  /** Get the number of sources added. */
  public int getSourceCount() {
    return sources.size();
  }

  /** Get the number of completed sources. */
  public int getCompletedSourceCount() {
    return completedSources.get();
  }
}
