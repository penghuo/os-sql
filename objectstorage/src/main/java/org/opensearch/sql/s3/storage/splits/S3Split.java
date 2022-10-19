/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage.splits;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.s3.storage.OSS3Object;
import org.opensearch.sql.storage.splits.Split;

public class S3Split extends Split {
  @Getter
  private final List<OSS3Object> objects;

  public S3Split(List<OSS3Object> objects) {
    super();
    this.objects = objects;
  }

//  public S3Split(String json) {
//    super();
//    try {
//      objects = OBJECT_MAPPER.readValue(json, new TypeReference<List<OSS3Object>>(){});
//    } catch (JsonProcessingException e) {
//      throw new RuntimeException(e);
//    }
//  }

  @Override
  public boolean onLocalNode() {
    return false;
  }

//  @Override
//  public String toJson() {
//    try {
//      return OBJECT_MAPPER.writeValueAsString(objects);
//    } catch (JsonProcessingException e) {
//      throw new RuntimeException(e);
//    }
//  }
}
