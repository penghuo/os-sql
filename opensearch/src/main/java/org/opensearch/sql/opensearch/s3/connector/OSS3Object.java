/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.connector;

public class OSS3Object {
  private final String bucket;

  private final String object;

  public OSS3Object(String bucket, String object) {
    this.bucket = bucket;
    this.object = object;
  }

  @Override
  public String toString() {
    return "OSS3Object{" +
        "bucket='" + bucket + '\'' +
        ", object='" + object + '\'' +
        '}';
  }

  public String getBucket() {
    return bucket;
  }

  public String getObject() {
    return object;
  }
}
