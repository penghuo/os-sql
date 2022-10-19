/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stage;

import lombok.Data;
import org.opensearch.sql.planner.logical.LogicalPlan;

@Data
public class StagePlan {

  private final StageId stageId;

  private final LogicalPlan plan;
}
