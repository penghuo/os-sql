/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.connector;

import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.s3.split.S3Split;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class S3ScanOperator extends TableScanOperator {

  private static final Logger log = LogManager.getLogger(S3ScanOperator.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Iterator<OSS3Object> partitions;

  private final S3Reader s3Reader;

  public S3ScanOperator() {
    this.s3Reader = new S3Reader();
  }

  @Override
  public String explain() {
    return "S3ScanOperator";
  }

  @Override
  public boolean hasNext() {
    if (s3Reader.hasNext()) {
      return true;
    } else if (!partitions.hasNext()) {
      return false;
    } else {
      s3Reader.close();
      final OSS3Object next = partitions.next();
      log.info("next file {}", next);
      s3Reader.open(next);
      return s3Reader.hasNext();
    }
  }

  @Override
  public ExprValue next() {
    TypeReference<Map<String,Object>> typeRef = new TypeReference<>() {};
    try {
      return tupleValue(OBJECT_MAPPER.readValue(s3Reader.next(), typeRef));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("S3ScanOperator exception", e);
    }
  }

  @Override
  public void addSplits(Split split) {
    this.partitions = ((S3Split) split).getObjects().iterator();
  }

  public void open() {
    OSS3Object next = this.partitions.next();

    log.info("next file {}", next);
    s3Reader.open(next);
  }

  @Override
  public void close() {
    s3Reader.close();
  }
}
