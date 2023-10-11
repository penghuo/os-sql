/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import lombok.Data;
import org.opensearch.index.seqno.SequenceNumbers;

/**
 * IndexStateStore Model.
 */
@Data
public class StateModel {
  private final String docId;
  private final String source;
  private final long seqNo;
  private final long primaryTerm;
}
