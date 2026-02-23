/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * General semantic analysis error: unresolved column, ambiguous reference, non-sortable type in
 * ORDER BY, etc. Extends the shared {@link DqeException} base from dqe-parser.
 */
public class DqeAnalysisException extends DqeException {

  public DqeAnalysisException(String message) {
    super(message, DqeErrorCode.ANALYSIS_ERROR);
  }

  public DqeAnalysisException(String message, Throwable cause) {
    super(message, DqeErrorCode.ANALYSIS_ERROR, cause);
  }
}
