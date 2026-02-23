/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Memory tracking, circuit breaker integration, and admission control for the DQE (Distributed
 * Query Engine).
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link org.opensearch.dqe.memory.DqeMemoryTracker} — wraps OpenSearch CircuitBreaker for
 *       all DQE memory allocations
 *   <li>{@link org.opensearch.dqe.memory.DqeCircuitBreakerRegistrar} — registers the "dqe" circuit
 *       breaker with OpenSearch
 *   <li>{@link org.opensearch.dqe.memory.AdmissionController} — node-level semaphore limiting
 *       concurrent DQE queries
 * </ul>
 */
package org.opensearch.dqe.memory;
