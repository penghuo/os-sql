/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job.exceptions;

/**
 * Thrown when the owner node has no capacity left to retain a query job or its result. The client
 * may retry once retained jobs are deleted or expire. The message names only the exhausted setting.
 */
public final class QueryJobCapacityException extends RuntimeException {

  /**
   * @param message client-safe description that names the exhausted setting
   */
  public QueryJobCapacityException(String message) {
    super(message);
  }
}
