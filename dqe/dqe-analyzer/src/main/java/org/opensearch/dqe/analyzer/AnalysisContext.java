/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.metadata.DqeMetadata;

/**
 * Mutable context threaded through the analysis visitor tree. Carries the current scope stack,
 * metadata handle, and security context.
 */
public class AnalysisContext {

  private final DqeMetadata metadata;
  private final SecurityContext securityContext;
  private final Deque<Scope> scopeStack;

  public AnalysisContext(DqeMetadata metadata, SecurityContext securityContext) {
    this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    this.securityContext =
        Objects.requireNonNull(securityContext, "securityContext must not be null");
    this.scopeStack = new ArrayDeque<>();
  }

  public DqeMetadata getMetadata() {
    return metadata;
  }

  public SecurityContext getSecurityContext() {
    return securityContext;
  }

  public void pushScope(Scope scope) {
    scopeStack.push(scope);
  }

  public Scope popScope() {
    return scopeStack.pop();
  }

  public Scope getCurrentScope() {
    if (scopeStack.isEmpty()) {
      throw new DqeAnalysisException("No scope available — analysis context has no active scope");
    }
    return scopeStack.peek();
  }
}
