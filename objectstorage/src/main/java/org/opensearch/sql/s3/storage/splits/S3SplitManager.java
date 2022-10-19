/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage.splits;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.s3.storage.OSS3Object;
import org.opensearch.sql.storage.splits.Split;
import org.opensearch.sql.storage.splits.SplitManager;

@RequiredArgsConstructor
public class S3SplitManager implements SplitManager {

  private final int PARTITION_NUMBER = 1;

  private final Set<OSS3Object> objects;

  @Override
  public List<Split> nextBatch() {
    int partitionSize = Math.max(1, objects.size() / PARTITION_NUMBER);
    List<Split> splits = new ArrayList<>();
    Iterables.partition(objects, partitionSize)
        .forEach(oss3Objects -> splits.add(new S3Split(oss3Objects)));
    return splits;
  }

  @Override
  public void close() {
    // do nothing.
  }
}
