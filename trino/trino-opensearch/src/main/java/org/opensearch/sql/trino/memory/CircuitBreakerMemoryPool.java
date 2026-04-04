/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.memory;

import io.airlift.units.DataSize;
import io.trino.memory.MemoryPool;

/**
 * MemoryPool that will delegate to OpenSearch circuit breaker in production. For PoC, uses parent
 * class accounting with fixed size.
 */
public class CircuitBreakerMemoryPool extends MemoryPool {

  public static CircuitBreakerMemoryPool create(DataSize maxSize) {
    return new CircuitBreakerMemoryPool(maxSize);
  }

  private CircuitBreakerMemoryPool(DataSize maxSize) {
    super(maxSize);
  }
}
