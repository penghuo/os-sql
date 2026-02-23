/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.security;

import org.opensearch.dqe.analyzer.security.SecurityContext;

/**
 * Phase 1 permissive security context that allows all index access. Full integration with the
 * OpenSearch Security plugin (checking actual user permissions) is deferred to Phase 2.
 *
 * <p>In Phase 1, all queries run with the permissions of the authenticated user, but this context
 * does not enforce fine-grained index-level access control. The REST layer already enforces cluster
 * transport-level authentication.
 */
public class PermissiveSecurityContext implements SecurityContext {

  private final String userName;

  /**
   * Creates a permissive security context for the given user.
   *
   * @param userName the authenticated user name
   */
  public PermissiveSecurityContext(String userName) {
    this.userName = userName != null ? userName : "anonymous";
  }

  @Override
  public boolean hasIndexReadPermission(String indexName) {
    // Phase 1: trust the REST layer authentication. Fine-grained index-level
    // permission checking will be integrated with the Security plugin in Phase 2.
    return true;
  }

  @Override
  public String getUserName() {
    return userName;
  }
}
