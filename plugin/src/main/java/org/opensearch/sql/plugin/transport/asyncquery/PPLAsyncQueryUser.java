/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.util.List;
import java.util.Objects;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.rest.RestStatus;

/**
 * Immutable submitter identity used to authorize retained asynchronous query state.
 *
 * @param securityEnabled whether the security plugin supplied an authenticated identity
 * @param name authenticated principal
 * @param requestedTenant requested security tenant
 * @param backendRoles backend roles captured at submission
 */
public record PPLAsyncQueryUser(
    boolean securityEnabled, String name, String requestedTenant, List<String> backendRoles) {

  /**
   * Creates an immutable asynchronous query identity.
   *
   * @param securityEnabled whether the security plugin supplied an authenticated identity
   * @param name authenticated principal
   * @param requestedTenant requested security tenant
   * @param backendRoles backend roles captured at submission
   */
  public PPLAsyncQueryUser {
    backendRoles = backendRoles == null ? List.of() : List.copyOf(backendRoles);
    if (securityEnabled && (name == null || name.isBlank())) {
      throw new IllegalArgumentException("Security-enabled PPL user must have a principal");
    }
  }

  /**
   * Captures the current caller from the OpenSearch thread context.
   *
   * @param threadContext current request thread context
   * @return immutable caller identity
   */
  public static PPLAsyncQueryUser current(ThreadContext threadContext) {
    try {
      Object serialized =
          threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
      if (serialized == null) {
        return new PPLAsyncQueryUser(false, null, null, List.of());
      }
      User user =
          serialized instanceof User currentUser
              ? currentUser
              : serialized instanceof String value ? User.parse(value) : null;
      if (user == null) {
        throw forbidden();
      }
      return new PPLAsyncQueryUser(
          true, user.getName(), user.getRequestedTenant(), user.getBackendRoles());
    } catch (RuntimeException e) {
      throw forbidden();
    }
  }

  void authorize(PPLAsyncQueryUser caller) {
    if (!securityEnabled && !caller.securityEnabled) {
      return;
    }
    if (!securityEnabled
        || !caller.securityEnabled
        || !Objects.equals(name, caller.name)
        || !Objects.equals(requestedTenant, caller.requestedTenant)
        || !caller.backendRoles.containsAll(backendRoles)) {
      throw forbidden();
    }
  }

  private static OpenSearchSecurityException forbidden() {
    return new OpenSearchSecurityException(
        "Not authorized to access PPL asynchronous query", RestStatus.FORBIDDEN);
  }
}
