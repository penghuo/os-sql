/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.split;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.s3.connector.OSS3Object;
import org.opensearch.sql.opensearch.s3.connector.S3Lister;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;

@RequiredArgsConstructor
public class S3SplitManager implements SplitManager {

  private final String tableName;

  static private int PARTITION = 1;

  @Override
  public CompletableFuture<List<Split>> nextBatch() {
    try {
      S3Lister s3Lister = new S3Lister(new URI(tableName));
      Iterable<List<OSS3Object>> partition = s3Lister.partition(PARTITION);
      List<Split> result = new ArrayList<>();
      for (List<OSS3Object> oss3Objects : partition) {
        final S3Split s3Split = new S3Split(oss3Objects);
        result.add(s3Split);
      }
      return CompletableFuture.completedFuture(result);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {

  }
}
