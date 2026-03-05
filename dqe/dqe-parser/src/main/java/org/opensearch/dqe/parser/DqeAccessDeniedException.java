/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/** Thrown when the user lacks permission to access a referenced index, table, or field. */
public class DqeAccessDeniedException extends DqeException {

  private final String resource;

  /**
   * Creates an access denied exception.
   *
   * @param resource the resource (index/table/field) that access was denied to
   */
  public DqeAccessDeniedException(String resource) {
    super("Access denied to '" + resource + "'", DqeErrorCode.ACCESS_DENIED);
    this.resource = resource;
  }

  /**
   * Creates an access denied exception with additional detail.
   *
   * @param resource the resource that access was denied to
   * @param detail additional detail (e.g., "requires read permission")
   */
  public DqeAccessDeniedException(String resource, String detail) {
    super("Access denied to '" + resource + "': " + detail, DqeErrorCode.ACCESS_DENIED);
    this.resource = resource;
  }

  /** Returns the resource name that access was denied to. */
  public String getResource() {
    return resource;
  }
}
