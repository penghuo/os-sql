/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("AnalysisContext")
class AnalysisContextTest {

  @Test
  @DisplayName("Push and pop scope")
  void pushPopScope() {
    DqeMetadata metadata = mock(DqeMetadata.class);
    SecurityContext security = mock(SecurityContext.class);
    AnalysisContext ctx = new AnalysisContext(metadata, security);

    DqeTableHandle table = new DqeTableHandle("test", null, List.of("test"), 1L, null);
    DqeColumnHandle col = new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, null, false);
    Scope scope = new Scope(table, List.of(col), Optional.empty());

    ctx.pushScope(scope);
    assertSame(scope, ctx.getCurrentScope());
    assertSame(scope, ctx.popScope());
  }

  @Test
  @DisplayName("getCurrentScope throws when empty")
  void emptyStackThrows() {
    DqeMetadata metadata = mock(DqeMetadata.class);
    SecurityContext security = mock(SecurityContext.class);
    AnalysisContext ctx = new AnalysisContext(metadata, security);

    assertThrows(DqeAnalysisException.class, ctx::getCurrentScope);
  }

  @Test
  @DisplayName("Getters return injected dependencies")
  void gettersReturnDependencies() {
    DqeMetadata metadata = mock(DqeMetadata.class);
    SecurityContext security = mock(SecurityContext.class);
    AnalysisContext ctx = new AnalysisContext(metadata, security);

    assertSame(metadata, ctx.getMetadata());
    assertSame(security, ctx.getSecurityContext());
  }

  @Test
  @DisplayName("Null metadata throws")
  void nullMetadataThrows() {
    SecurityContext security = mock(SecurityContext.class);
    assertThrows(NullPointerException.class, () -> new AnalysisContext(null, security));
  }

  @Test
  @DisplayName("Null security context throws")
  void nullSecurityThrows() {
    DqeMetadata metadata = mock(DqeMetadata.class);
    assertThrows(NullPointerException.class, () -> new AnalysisContext(metadata, null));
  }
}
