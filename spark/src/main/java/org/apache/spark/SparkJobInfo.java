/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark;

import java.io.Serializable;

/**
 * Exposes information about Spark Jobs.
 *
 * This interface is not designed to be implemented outside of Spark.  We may add additional methods
 * which may break binary compatibility with outside implementations.
 */
public interface SparkJobInfo extends Serializable {
  int jobId();
  int[] stageIds();
  JobExecutionStatus status();
}
