/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Gather exchange pattern: all shard results flow to a single coordinator buffer.
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link org.opensearch.dqe.exchange.gather.GatherExchangeSource} — coordinator-side,
 *       receives pages from all shards
 *   <li>{@link org.opensearch.dqe.exchange.gather.GatherExchangeSink} — shard-side, pushes pages to
 *       coordinator
 * </ul>
 */
package org.opensearch.dqe.exchange.gather;
