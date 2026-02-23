/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Exchange buffer with backpressure and sequence number deduplication.
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link org.opensearch.dqe.exchange.buffer.ExchangeBuffer} — bounded buffer between
 *       producers and consumers
 *   <li>{@link org.opensearch.dqe.exchange.buffer.BackpressureController} — producer flow control
 *   <li>{@link org.opensearch.dqe.exchange.buffer.SequenceTracker} — duplicate chunk detection
 * </ul>
 */
package org.opensearch.dqe.exchange.buffer;
