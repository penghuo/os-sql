/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/**
 * Stable error codes for all DQE exceptions. Used in REST error responses and error catalogs. All
 * DQE modules share this single enum, defined in dqe-parser since it is the foundational module
 * with no internal dependencies.
 */
public enum DqeErrorCode {

  // Parser (dqe-parser)
  /** SQL syntax error detected by the parser. */
  PARSING_ERROR,

  // Analyzer (dqe-analyzer)
  /** SQL construct recognized by parser but not supported by DQE in the current phase. */
  UNSUPPORTED_OPERATION,
  /** Type mismatch detected during analysis (column type vs expression type). */
  TYPE_MISMATCH,
  /** User lacks permission to access the referenced index or field. */
  ACCESS_DENIED,
  /** General semantic analysis error. */
  ANALYSIS_ERROR,

  // Metadata (dqe-metadata)
  /** Referenced table/index does not exist. */
  TABLE_NOT_FOUND,
  /** Required shard is not available (unassigned or relocating). */
  SHARD_NOT_AVAILABLE,

  // Execution (dqe-execution)
  /** Runtime execution failure (overflow, division by zero, cast failure). */
  EXECUTION_ERROR,
  /** Query was cancelled by user or system. */
  QUERY_CANCELLED,
  /** Query exceeded the configured timeout. */
  QUERY_TIMEOUT,
  /** Point-in-time snapshot expired during query execution. */
  PIT_EXPIRED,

  // Memory (dqe-memory)
  /** Query exceeded its per-query memory budget. */
  EXCEEDED_QUERY_MEMORY_LIMIT,
  /** OpenSearch circuit breaker tripped. */
  CIRCUIT_BREAKER_TRIPPED,
  /** Too many concurrent DQE queries running. */
  TOO_MANY_CONCURRENT_QUERIES,

  // Exchange (dqe-exchange)
  /** Exchange buffer timed out waiting for data. */
  EXCHANGE_BUFFER_TIMEOUT,
  /** A stage failed during distributed execution. */
  STAGE_EXECUTION_FAILED,

  // Plugin (dqe-plugin)
  /** DQE engine is disabled via cluster setting. */
  DQE_DISABLED,
  /** Invalid request format or parameters. */
  INVALID_REQUEST,

  // General
  /** Internal/unexpected error. */
  INTERNAL_ERROR
}
