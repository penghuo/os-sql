/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;

/**
 * Produces an output Block from an input Page. Can either select an existing column or compute a
 * derived expression.
 */
@FunctionalInterface
public interface PageProjection {

  /**
   * Produces an output block from the input page for the given selected positions.
   *
   * @param page the input page
   * @param selectedPositions boolean mask of which positions to include
   * @return the projected output block containing only selected positions
   */
  Block project(Page page, boolean[] selectedPositions);
}
