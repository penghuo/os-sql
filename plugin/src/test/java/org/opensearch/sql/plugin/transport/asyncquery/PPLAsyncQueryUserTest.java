/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;

public class PPLAsyncQueryUserTest {

  @Test
  public void capturesSecurityIdentity() {
    ThreadContext context = new ThreadContext(Settings.EMPTY);
    context.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
        "alice|backend-a|ppl-role|tenant-a");

    PPLAsyncQueryUser identity = new PPLAsyncQuerySecurity(true).currentUser(context);

    assertEquals("alice", identity.name());
    assertEquals("tenant-a", identity.requestedTenant());
    assertEquals(List.of("backend-a"), identity.backendRoles());
  }

  @Test
  public void acceptsUserObjectAndRejectsUnknownSecurityContext() {
    ThreadContext objectContext = new ThreadContext(Settings.EMPTY);
    objectContext.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
        new User("alice", List.of("backend-a"), List.of("ppl-role"), null, "tenant-a"));
    PPLAsyncQuerySecurity security = new PPLAsyncQuerySecurity(true);
    assertEquals("alice", security.currentUser(objectContext).name());

    ThreadContext invalidContext = new ThreadContext(Settings.EMPTY);
    invalidContext.putTransient(
        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, new Object());
    assertThrows(OpenSearchSecurityException.class, () -> security.currentUser(invalidContext));
  }

  @Test
  public void missingIdentityFailsClosedWhenSecurityIsEnabled() {
    ThreadContext context = new ThreadContext(Settings.EMPTY);

    assertThrows(
        OpenSearchSecurityException.class,
        () -> new PPLAsyncQuerySecurity(true).currentUser(context));
    assertFalse(new PPLAsyncQuerySecurity(false).currentUser(context).securityEnabled());
  }

  @Test
  public void requiresSamePrincipalTenantAndOriginalBackendRoles() {
    PPLAsyncQueryUser owner = new PPLAsyncQueryUser(true, "alice", "tenant-a", List.of("role-a"));

    owner.authorize(
        new PPLAsyncQueryUser(true, "alice", "tenant-a", List.of("role-a", "newly-added-role")));

    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser(true, "bob", "tenant-a", List.of("role-a"))));
    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser(true, "alice", "tenant-b", List.of("role-a"))));
    assertThrows(
        OpenSearchSecurityException.class,
        () -> owner.authorize(new PPLAsyncQueryUser(true, "alice", "tenant-a", List.of())));
  }

  @Test
  public void unsecuredModeRequiresAnUnsecuredCaller() {
    PPLAsyncQueryUser unsecured = new PPLAsyncQueryUser(false, null, null, List.of());
    unsecured.authorize(new PPLAsyncQueryUser(false, null, null, List.of()));

    assertThrows(
        OpenSearchSecurityException.class,
        () -> unsecured.authorize(new PPLAsyncQueryUser(true, "alice", null, List.of("role-a"))));
  }
}
