/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.request;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Immutable DQE query request produced by {@link DqeRequestParser}. Contains all fields extracted
 * from the REST request body: query, engine, fetch_size, session properties, and optional overrides
 * for max memory and timeout.
 */
public class DqeQueryRequest {

  private final String query;
  private final String engine;
  private final int fetchSize;
  private final Map<String, String> sessionProperties;
  private final String queryId;
  private final Long queryMaxMemoryBytes;
  private final Duration queryTimeout;

  private DqeQueryRequest(Builder builder) {
    this.query = builder.query;
    this.engine = builder.engine;
    this.fetchSize = builder.fetchSize;
    this.sessionProperties =
        builder.sessionProperties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(builder.sessionProperties);
    this.queryId = builder.queryId != null ? builder.queryId : UUID.randomUUID().toString();
    this.queryMaxMemoryBytes = builder.queryMaxMemoryBytes;
    this.queryTimeout = builder.queryTimeout;
  }

  /** Convenience constructor for simple cases (tests, routing). */
  public DqeQueryRequest(String query, String engine) {
    this.query = query;
    this.engine = engine;
    this.fetchSize = 0;
    this.sessionProperties = Collections.emptyMap();
    this.queryId = UUID.randomUUID().toString();
    this.queryMaxMemoryBytes = null;
    this.queryTimeout = null;
  }

  public String getQuery() {
    return query;
  }

  public String getEngine() {
    return engine;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public Map<String, String> getSessionProperties() {
    return sessionProperties;
  }

  public String getQueryId() {
    return queryId;
  }

  public Optional<Long> getQueryMaxMemoryBytes() {
    return Optional.ofNullable(queryMaxMemoryBytes);
  }

  public Optional<Duration> getQueryTimeout() {
    return Optional.ofNullable(queryTimeout);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for DqeQueryRequest. */
  public static class Builder {
    private String query;
    private String engine = "dqe";
    private int fetchSize;
    private Map<String, String> sessionProperties;
    private String queryId;
    private Long queryMaxMemoryBytes;
    private Duration queryTimeout;

    public Builder query(String query) {
      this.query = query;
      return this;
    }

    public Builder engine(String engine) {
      this.engine = engine;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder sessionProperties(Map<String, String> sessionProperties) {
      this.sessionProperties = sessionProperties;
      return this;
    }

    public Builder queryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public Builder queryMaxMemoryBytes(Long queryMaxMemoryBytes) {
      this.queryMaxMemoryBytes = queryMaxMemoryBytes;
      return this;
    }

    public Builder queryTimeout(Duration queryTimeout) {
      this.queryTimeout = queryTimeout;
      return this;
    }

    public DqeQueryRequest build() {
      return new DqeQueryRequest(this);
    }
  }
}
