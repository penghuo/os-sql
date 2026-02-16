/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.Collections;
import java.util.List;
import lombok.Data;

/**
 * Specification for a data exchange between two fragments. Describes how rows are shuffled from the
 * producing fragment to the consuming fragment.
 */
@Data
public class ExchangeSpec {

  /** Default buffer size for exchange data transfer (10 MB). */
  private static final long DEFAULT_BUFFER_SIZE_BYTES = 10L * 1024 * 1024;

  /** The distribution strategy for this exchange. */
  private final ExchangeType type;

  /** Fields used for ordering when the exchange must preserve sort order. */
  private final List<String> orderByFields;

  /** Keys used for hash-partitioning when {@link #type} is {@link ExchangeType#HASH}. */
  private final List<String> partitionKeys;

  /** Buffer size in bytes for exchange data transfer. */
  private final long bufferSizeBytes;

  /**
   * Creates a GATHER exchange that collects all data to a single destination.
   *
   * @return a gather exchange specification
   */
  public static ExchangeSpec gather() {
    return new ExchangeSpec(
        ExchangeType.GATHER,
        Collections.emptyList(),
        Collections.emptyList(),
        DEFAULT_BUFFER_SIZE_BYTES);
  }

  /**
   * Creates a HASH exchange that partitions data by the given keys.
   *
   * @param keys the partition keys
   * @return a hash exchange specification
   */
  public static ExchangeSpec hash(List<String> keys) {
    return new ExchangeSpec(
        ExchangeType.HASH, Collections.emptyList(), keys, DEFAULT_BUFFER_SIZE_BYTES);
  }

  /**
   * Creates a GATHER exchange that preserves the sort order of the given fields.
   *
   * @param sortFields the fields defining the sort order
   * @return an ordered gather exchange specification
   */
  public static ExchangeSpec orderedGather(List<String> sortFields) {
    return new ExchangeSpec(
        ExchangeType.GATHER, sortFields, Collections.emptyList(), DEFAULT_BUFFER_SIZE_BYTES);
  }

  /**
   * Creates a BROADCAST exchange that replicates every row to all destinations.
   *
   * @return a broadcast exchange specification
   */
  public static ExchangeSpec broadcast() {
    return new ExchangeSpec(
        ExchangeType.BROADCAST,
        Collections.emptyList(),
        Collections.emptyList(),
        DEFAULT_BUFFER_SIZE_BYTES);
  }
}
