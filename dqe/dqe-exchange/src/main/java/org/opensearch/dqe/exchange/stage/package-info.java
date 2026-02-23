/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Stage scheduling, execution, and cancellation for the DQE distributed query engine.
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link org.opensearch.dqe.exchange.stage.StageScheduler} — coordinator-side, dispatches
 *       plan fragments to data nodes
 *   <li>{@link org.opensearch.dqe.exchange.stage.StageExecutionHandler} — data-node-side, receives
 *       and executes stage requests
 *   <li>{@link org.opensearch.dqe.exchange.stage.StageCancelHandler} — data-node-side, handles
 *       stage cancellation requests
 *   <li>{@link org.opensearch.dqe.exchange.stage.StageScheduleResult} — result of dispatching a
 *       stage to data nodes
 * </ul>
 */
package org.opensearch.dqe.exchange.stage;
