/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api;

import java.util.Map;
import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for building shuffle support modules for the Driver.
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * Called once in the driver to bootstrap this module that is specific to this application.
   * This method is called before submitting executor requests to the cluster manager.
   *
   * This method should prepare the module with its shuffle components i.e. registering against
   * an external file servers or shuffle services, or creating tables in a shuffle
   * storage data database.
   *
   * @return additional SparkConf settings necessary for initializing the executor components.
   * This would include configurations that cannot be statically set on the application, like
   * the host:port of external services for shuffle storage.
   */
  Map<String, String> initializeApplication();

  /**
   * Called once at the end of the Spark application to clean up any existing shuffle state.
   */
  void cleanupApplication();

  /**
   * Called once per shuffle id when the shuffle id is first generated for a shuffle stage.
   *
   * @param shuffleId The unique identifier for the shuffle stage.
   */
  default void registerShuffle(int shuffleId) {}

  /**
   * Removes shuffle data associated with the given shuffle.
   *
   * @param shuffleId The unique identifier for the shuffle stage.
   * @param blocking Whether this call should block on the deletion of the data.
   */
  default void removeShuffle(int shuffleId, boolean blocking) {}
}
