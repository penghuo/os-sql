/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.data.Page;

/**
 * An operator that consumes Pages without producing output. Sits at the end of a pipeline (e.g.,
 * writing to an output buffer).
 */
public interface SinkOperator extends Operator {

  @Override
  default Page getOutput() {
    return null;
  }
}
