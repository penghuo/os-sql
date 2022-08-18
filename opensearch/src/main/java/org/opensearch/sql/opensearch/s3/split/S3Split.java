/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.split;

import java.util.List;
import lombok.Getter;
import org.opensearch.sql.opensearch.s3.connector.OSS3Object;
import org.opensearch.sql.planner.splits.Split;

public class S3Split implements Split {
  @Getter
  private final List<OSS3Object> objects;

  public S3Split(List<OSS3Object> objects) {
    this.objects = objects;
  }
}
