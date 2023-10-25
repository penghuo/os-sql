/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;

/**
 * Cancel refreshing job.
 */
public class FlintIndexOpCancel extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final EMRServerlessClient emrServerlessClient;
  private final String applicationId;
  private final String jobId;

  public FlintIndexOpCancel(StateStore stateStore, String datasourceName,
                            EMRServerlessClient emrServerlessClient, String applicationId,
                            String jobId) {
    super(stateStore, datasourceName);
    this.emrServerlessClient = emrServerlessClient;
    this.applicationId = applicationId;
    this.jobId = jobId;
  }

  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.REFRESHING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.CANCELLING;
  }

  /**
   * cancel EMR-S job, wait cancelled state upto 15s.
   */
  @SneakyThrows
  @Override
  void runOp() {
    // todo, cancel job failed, job does not exist?
    emrServerlessClient.cancelJobRun(applicationId, jobId);
    String jobRunState = "";
    int count = 3;
    do {
      jobRunState =
          emrServerlessClient.getJobRunResult(applicationId, jobId).getJobRun().getState();
      TimeUnit.SECONDS.sleep(5);
    } while (--count == 0 || jobRunState.equalsIgnoreCase("Cancelled"));

    if (!jobRunState.equalsIgnoreCase("Cancelled")) {
      String errMsg = "cancel job timeout timeout";
      LOG.error(errMsg);
      throw new IllegalStateException(errMsg);
    }
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.ACTIVE;
  }
}
