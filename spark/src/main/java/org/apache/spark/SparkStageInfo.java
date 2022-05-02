/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark;

import java.io.Serializable;

/**
 * Exposes information about Spark Stages.
 *
 * This interface is not designed to be implemented outside of Spark.  We may add additional methods
 * which may break binary compatibility with outside implementations.
 */
public interface SparkStageInfo extends Serializable {
  int stageId();
  int currentAttemptId();
  long submissionTime();
  String name();
  int numTasks();
  int numActiveTasks();
  int numCompletedTasks();
  int numFailedTasks();
}
