/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.calcite;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * "Iterator" which retrieves results lazily and in batches. Uses
 * <a href="https://www.elastic.co/guide/en/OpenSearch/reference/current/search-request-scroll.html">Elastic Scrolling API</a>
 * to optimally consume large search results.
 *
 * <p>This class is <strong>not thread safe</strong>.
 */
class Scrolling {

  private final OpenSearchTransport transport;
  private final int fetchSize;

  Scrolling(OpenSearchTransport transport) {
    this.transport = requireNonNull(transport, "transport");
    final int fetchSize = transport.fetchSize;
    checkArgument(fetchSize > 0,
        "invalid fetch size. Expected %s > 0", fetchSize);
    this.fetchSize = fetchSize;
  }

  Iterator<OpenSearchJson.SearchHit> query(ObjectNode query) {
    requireNonNull(query, "query");
    final long limit;
    if (query.has("size")) {
      limit = query.get("size").asLong();
      if (fetchSize > limit) {
        // don't use scrolling when batch size is greater than limit
        return transport.search().apply(query).searchHits().hits().iterator();
      }
    } else {
      limit = Long.MAX_VALUE;
    }

    query.put("size", fetchSize);
    final OpenSearchJson.Result first = transport
        .search(Collections.singletonMap("scroll", "1m")).apply(query);

    AutoClosingIterator iterator =
        new AutoClosingIterator(new SequentialIterator(first, transport, limit),
            scrollId -> transport.closeScroll(Collections.singleton(scrollId)));

    Iterator<OpenSearchJson.SearchHit> result = flatten(iterator);
    // apply limit
    if (limit != Long.MAX_VALUE) {
      result = Iterators.limit(result, (int) limit);
    }

    return result;
  }

  /**
   * Combines lazily multiple {@link OpenSearchJson.Result} into a single iterator of
   * {@link OpenSearchJson.SearchHit}.
   */
  private static Iterator<OpenSearchJson.SearchHit> flatten(
      Iterator<OpenSearchJson.Result> results) {
    final Iterator<Iterator<OpenSearchJson.SearchHit>> inputs =
        Iterators.transform(results,
            input -> input.searchHits().hits().iterator());
    return Iterators.concat(inputs);
  }

  /**
   * Observes when existing iterator has ended and clears context (scroll) if any.
   */
  private static class AutoClosingIterator implements Iterator<OpenSearchJson.Result>,
      AutoCloseable {
    private final Iterator<OpenSearchJson.Result> delegate;
    private final Consumer<String> closer;

    /** Returns whether {@link #closer} consumer was already called. */
    private boolean closed;

    /** Keeps last value of {@code scrollId} in memory so scroll can be released
     * upon termination. */
    private String scrollId;

    private AutoClosingIterator(
        final Iterator<OpenSearchJson.Result> delegate,
        final Consumer<String> closer) {
      this.delegate = delegate;
      this.closer = closer;
    }

    @Override public void close() {
      if (!closed && scrollId != null) {
        // close once (if scrollId is present)
        closer.accept(scrollId);
      }
      closed = true;
    }

    @Override public boolean hasNext() {
      final boolean hasNext = delegate.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    }

    @Override public OpenSearchJson.Result next() {
      OpenSearchJson.Result next = delegate.next();
      next.scrollId().ifPresent(id -> scrollId = id);
      return next;
    }
  }

  /**
   * Iterator which consumes current {@code scrollId} until full search result is fetched
   * or {@code limit} is reached.
   */
  private static class SequentialIterator
      extends AbstractSequentialIterator<OpenSearchJson.Result> {

    private final OpenSearchTransport transport;
    private final long limit;
    private long count;

    private SequentialIterator(final OpenSearchJson.Result first,
                               final OpenSearchTransport transport, final long limit) {
      super(first);
      this.transport = transport;
      checkArgument(limit >= 0,
          "limit: %s >= 0", limit);
      this.limit = limit;
    }

    @Override protected OpenSearchJson.Result computeNext(
        final OpenSearchJson.Result previous) {
      final int hits = previous.searchHits().hits().size();
      if (hits == 0 || count >= limit) {
        // stop (re-)requesting when limit is reached or no more results
        return null;
      }

      count += hits;
      final String scrollId = previous.scrollId()
          .orElseThrow(() -> new IllegalStateException("scrollId has to be present"));

      return transport.scroll().apply(scrollId);
    }
  }
}
