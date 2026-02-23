/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Transport action definitions for the DQE exchange protocol.
 *
 * <p>This package defines the transport actions, request classes, and response classes for
 * distributed query execution:
 *
 * <ul>
 *   <li>{@code internal:dqe/exchange/push} — push data pages from producer to consumer
 *   <li>{@code internal:dqe/exchange/close} — producer signals completion
 *   <li>{@code internal:dqe/exchange/abort} — either side signals failure
 *   <li>{@code internal:dqe/stage/execute} — coordinator sends plan fragment to data node
 *   <li>{@code internal:dqe/stage/cancel} — coordinator cancels a running stage
 * </ul>
 */
package org.opensearch.dqe.exchange.action;
