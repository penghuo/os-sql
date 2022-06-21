/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3;

import java.io.IOException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

public class OSS3Object implements Writeable {
  private final String bucket;

  private final String object;

  public OSS3Object(String bucket, String object) {
    this.bucket = bucket;
    this.object = object;
  }

  public OSS3Object(StreamInput in) throws IOException {
    this.bucket = in.readString();
    this.object = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(bucket);
    out.writeString(object);
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
