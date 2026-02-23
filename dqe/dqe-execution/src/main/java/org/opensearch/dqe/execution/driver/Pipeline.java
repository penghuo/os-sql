/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import java.util.List;
import java.util.Objects;
import org.opensearch.dqe.execution.operator.Operator;

/** An ordered chain of operators from source to output. */
public class Pipeline {

  private final List<Operator> operators;

  public Pipeline(List<Operator> operators) {
    this.operators = List.copyOf(Objects.requireNonNull(operators, "operators must not be null"));
    if (this.operators.isEmpty()) {
      throw new IllegalArgumentException("Pipeline must contain at least one operator");
    }
  }

  /** Returns the source (first) operator. */
  public Operator getSource() {
    return operators.get(0);
  }

  /** Returns the output (last) operator. */
  public Operator getOutput() {
    return operators.get(operators.size() - 1);
  }

  /** Returns all operators in order. */
  public List<Operator> getOperators() {
    return operators;
  }

  /** Closes all operators in reverse order. */
  public void close() {
    for (int i = operators.size() - 1; i >= 0; i--) {
      try {
        operators.get(i).close();
      } catch (Exception e) {
        // Log but continue closing remaining operators
      }
    }
  }
}
