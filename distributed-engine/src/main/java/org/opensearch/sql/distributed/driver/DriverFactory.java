/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.operator.OperatorFactory;

/** Factory for creating Drivers from a list of OperatorFactories and a PipelineContext. */
public class DriverFactory {

  private final PipelineContext pipelineContext;

  public DriverFactory(PipelineContext pipelineContext) {
    this.pipelineContext = Objects.requireNonNull(pipelineContext, "pipelineContext is null");
  }

  /** Creates a Pipeline and uses it to produce a single Driver. */
  public Driver createDriver(List<OperatorFactory> operatorFactories) {
    Pipeline pipeline = new Pipeline(pipelineContext, operatorFactories);
    DriverContext driverContext = pipelineContext.addDriverContext();
    return pipeline.createDriver(driverContext);
  }

  /** Creates a Pipeline and produces multiple Drivers (e.g., one per shard). */
  public List<Driver> createDrivers(List<OperatorFactory> operatorFactories, int driverCount) {
    Pipeline pipeline = new Pipeline(pipelineContext, operatorFactories);
    return pipeline.createDrivers(driverCount);
  }
}
