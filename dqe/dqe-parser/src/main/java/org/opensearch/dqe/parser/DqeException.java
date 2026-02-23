/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/**
 * Base exception for all DQE errors. Carries a stable {@link DqeErrorCode} for structured error
 * responses. Extends {@link RuntimeException} (unchecked) following Trino convention — the plugin
 * layer catches these at the REST boundary.
 */
public class DqeException extends RuntimeException {

  private final DqeErrorCode errorCode;

  /**
   * Creates a new DQE exception.
   *
   * @param message human-readable error description
   * @param errorCode stable error code from {@link DqeErrorCode}
   */
  public DqeException(String message, DqeErrorCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  /**
   * Creates a new DQE exception with an underlying cause.
   *
   * @param message human-readable error description
   * @param errorCode stable error code from {@link DqeErrorCode}
   * @param cause the underlying cause
   */
  public DqeException(String message, DqeErrorCode errorCode, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  /** Returns the stable error code for this exception. */
  public DqeErrorCode getErrorCode() {
    return errorCode;
  }
}
