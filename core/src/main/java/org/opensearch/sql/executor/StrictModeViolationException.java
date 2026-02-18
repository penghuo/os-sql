/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

/**
 * Thrown when distributed engine strict mode is enabled and a supported query pattern fails to
 * execute through the distributed engine. In strict mode, only explicitly-unsupported patterns
 * (joins, window functions) are allowed to fall back; supported patterns that fail throw this
 * exception instead of silently falling back to the DSL path.
 */
public class StrictModeViolationException extends RuntimeException {

  public StrictModeViolationException(String message) {
    super(message);
  }

  public StrictModeViolationException(String message, Throwable cause) {
    super(message, cause);
  }
}
