/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("PermissiveSecurityContext")
class PermissiveSecurityContextTests {

  @Nested
  @DisplayName("hasIndexReadPermission")
  class HasIndexReadPermission {

    @Test
    @DisplayName("always returns true for any index")
    void alwaysReturnsTrue() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext("user1");
      assertTrue(ctx.hasIndexReadPermission("my_index"));
    }

    @Test
    @DisplayName("returns true for system indices")
    void trueForSystemIndices() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext("admin");
      assertTrue(ctx.hasIndexReadPermission(".opensearch-sap-log-types-config"));
    }

    @Test
    @DisplayName("returns true for wildcard patterns")
    void trueForWildcard() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext("user1");
      assertTrue(ctx.hasIndexReadPermission("logs-*"));
    }
  }

  @Nested
  @DisplayName("getUserName")
  class GetUserName {

    @Test
    @DisplayName("returns provided user name")
    void returnsProvidedName() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext("testuser");
      assertEquals("testuser", ctx.getUserName());
    }

    @Test
    @DisplayName("returns anonymous for null user name")
    void returnsAnonymousForNull() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext(null);
      assertEquals("anonymous", ctx.getUserName());
    }

    @Test
    @DisplayName("preserves non-null user name as-is")
    void preservesUserName() {
      PermissiveSecurityContext ctx = new PermissiveSecurityContext("admin@corp.com");
      assertEquals("admin@corp.com", ctx.getUserName());
    }
  }
}
