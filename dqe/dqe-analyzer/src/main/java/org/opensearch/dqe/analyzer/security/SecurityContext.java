/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.security;

/**
 * Wraps permission checking for the current user/request. Integrates with OpenSearch Security
 * plugin's permission framework. The dqe-plugin module provides the concrete implementation.
 */
public interface SecurityContext {

  /**
   * Check if the current user has read access to the specified index.
   *
   * @param indexName the OpenSearch index name
   * @return true if the user has read access
   */
  boolean hasIndexReadPermission(String indexName);

  /** Returns the authenticated user's name (for audit logging). */
  String getUserName();
}
