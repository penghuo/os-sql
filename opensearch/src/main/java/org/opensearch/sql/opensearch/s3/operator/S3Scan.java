/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.s3.OSS3Object;

public class S3Scan implements Iterator<Map<String, Object>> {
  private static final Logger log = LogManager.getLogger(S3Scan.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Iterator<OSS3Object> partitions;

  private S3Reader s3Reader;

  public S3Scan(List<OSS3Object> partitions) {
    this.partitions = partitions.iterator();
    this.s3Reader = new S3Reader();
  }

  public void open() {
    OSS3Object next = partitions.next();
    
    System.out.println("next file " + next);
    s3Reader.open(next);
  }

  // either s3Reader not been consumed or s3 objects still not been consumed.
  @Override
  public boolean hasNext() {
    if (s3Reader.hasNext()) {
      return true;
    } else if (!partitions.hasNext()) {
      return false;
    } else {
      s3Reader.close();
      final OSS3Object next = partitions.next();
      System.out.println("next file " + next);
      s3Reader.open(next);
      return s3Reader.hasNext();
    }
  }

  @SneakyThrows
  @Override
  public Map<String, Object> next() {
    TypeReference<Map<String,Object>> typeRef
        = new TypeReference<>() {};

    return OBJECT_MAPPER.readValue(s3Reader.next(), typeRef);
  }

  public void close() {
    s3Reader.close();
  }
}
