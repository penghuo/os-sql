/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

/**
 * Why a source stopped producing work. Both reasons mean the source is done, not that it failed.
 */
public enum CompletionReason {

  /** The source returned everything it had: end of stream. */
  EXHAUSTED,

  /**
   * A downstream operator stopped asking — a {@code head}, a limit, or the configured query size
   * limit. The source could have produced more, but nothing will consume it, so its remaining work
   * is not part of this query.
   */
  UPSTREAM_LIMIT
}
