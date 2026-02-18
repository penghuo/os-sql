/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;

/**
 * A pipeline is a chain of OperatorFactories that produces Drivers. Each Driver gets its own
 * operator instances created from the factories. Ported from Trino's pipeline concept.
 */
public class Pipeline {

  private final PipelineContext pipelineContext;
  private final List<OperatorFactory> operatorFactories;

  public Pipeline(PipelineContext pipelineContext, List<OperatorFactory> operatorFactories) {
    this.pipelineContext = Objects.requireNonNull(pipelineContext, "pipelineContext is null");
    this.operatorFactories =
        List.copyOf(Objects.requireNonNull(operatorFactories, "operatorFactories is null"));
    if (operatorFactories.size() < 2) {
      throw new IllegalArgumentException(
          "A Pipeline requires at least 2 operator factories (source + sink)");
    }
  }

  public PipelineContext getPipelineContext() {
    return pipelineContext;
  }

  public List<OperatorFactory> getOperatorFactories() {
    return operatorFactories;
  }

  /** Creates a single Driver with its own operator instances and driver context. */
  public Driver createDriver(DriverContext driverContext) {
    List<Operator> operators = new ArrayList<>();
    for (int i = 0; i < operatorFactories.size(); i++) {
      var operatorContext =
          driverContext.addOperatorContext(i, operatorFactories.get(i).getClass().getSimpleName());
      operators.add(operatorFactories.get(i).createOperator(operatorContext));
    }
    return new Driver(driverContext, operators, new DriverYieldSignal());
  }

  /** Creates multiple Drivers (e.g., one per shard). */
  public List<Driver> createDrivers(int count) {
    List<Driver> drivers = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      DriverContext driverContext = pipelineContext.addDriverContext();
      drivers.add(createDriver(driverContext));
    }
    return drivers;
  }

  /** Signals that no more drivers will be created from this pipeline. */
  public void noMoreDrivers() {
    for (OperatorFactory factory : operatorFactories) {
      factory.noMoreOperators();
    }
  }
}
