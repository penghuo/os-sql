/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.stage;

import java.util.UUID;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StageId {
  private final String id;

  public static StageId stageId() {
    return new StageId("stage-" + UUID.randomUUID());
  }
}
