/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/**
 * Thrown when a SQL construct is syntactically valid but not supported by DQE in the current phase
 * (e.g., GROUP BY, JOIN in Phase 1). Detected at analysis time, not parsing time.
 */
public class DqeUnsupportedOperationException extends DqeException {

  private final String construct;

  /**
   * Creates an unsupported operation exception.
   *
   * @param construct the unsupported SQL construct name (e.g., "GROUP BY", "JOIN")
   */
  public DqeUnsupportedOperationException(String construct) {
    super(
        "DQE does not support " + construct + ". See supported SQL reference.",
        DqeErrorCode.UNSUPPORTED_OPERATION);
    this.construct = construct;
  }

  /**
   * Creates an unsupported operation exception with additional detail.
   *
   * @param construct the unsupported SQL construct name
   * @param detail additional detail about the unsupported usage
   */
  public DqeUnsupportedOperationException(String construct, String detail) {
    super(
        "DQE does not support " + construct + ": " + detail + ". See supported SQL reference.",
        DqeErrorCode.UNSUPPORTED_OPERATION);
    this.construct = construct;
  }

  /** Returns the name of the unsupported SQL construct. */
  public String getConstruct() {
    return construct;
  }
}
