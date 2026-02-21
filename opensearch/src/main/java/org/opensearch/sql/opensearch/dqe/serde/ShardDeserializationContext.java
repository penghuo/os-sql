/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.serde;

import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * Thread-local holder that provides runtime context during shard-side plan deserialization. Custom
 * RelNode types (e.g., DSLScan) that require an {@link OpenSearchIndex} at construction time can
 * retrieve it from this context instead of relying on Calcite's standard {@code RelInput} which
 * does not carry OpenSearch-specific objects.
 *
 * <p>Usage: set before calling {@link RelNodeSerializer#deserialize}, clear after.
 */
public class ShardDeserializationContext {

  private static final ThreadLocal<ShardDeserializationContext> CURRENT = new ThreadLocal<>();

  private final OpenSearchIndex openSearchIndex;

  public ShardDeserializationContext(OpenSearchIndex openSearchIndex) {
    this.openSearchIndex = openSearchIndex;
  }

  public OpenSearchIndex getOpenSearchIndex() {
    return openSearchIndex;
  }

  /** Set the context for the current thread. */
  public static void set(ShardDeserializationContext ctx) {
    CURRENT.set(ctx);
  }

  /** Get the context for the current thread, or null if not set. */
  public static ShardDeserializationContext get() {
    return CURRENT.get();
  }

  /** Clear the context for the current thread. */
  public static void clear() {
    CURRENT.remove();
  }
}
