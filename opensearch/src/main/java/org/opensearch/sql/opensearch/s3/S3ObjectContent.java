/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.opensearch.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * S3ObjectContent Todo.
 */
class S3ObjectContent implements Iterator<String> {
  private static final Logger log = LogManager.getLogger(S3ObjectContent.class);

  /**
   * S3Client.
   */
  private final S3Client s3;

  /**
   * Current Iterator.
   */
  private Iterator<String> currIterator;

  private BufferedReader reader;

  public S3ObjectContent() {
    s3 = doPrivileged(() -> S3Client
        .builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .build());
  }

  public void open(Pair<String, String> s3Object)  {
    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
        .bucket(s3Object.getLeft())
        .key(s3Object.getRight())
        .build();

    ResponseBytes<GetObjectResponse> s3Objects = doPrivileged(() -> s3.getObjectAsBytes(getObjectRequest));
    try {
      reader =
          new BufferedReader(new InputStreamReader(new GZIPInputStream(s3Objects.asInputStream())));
      currIterator = reader.lines().iterator();
    } catch (IOException e) {
      log.error("failed to read s3 object {} ", s3Object, e);
      throw new RuntimeException(e);
    }
  }

  public void close() {
    currIterator = null;
    try {
      reader.close();
    } catch (IOException e) {
      log.error("failed to close read stream",e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return currIterator != null && currIterator.hasNext();
  }

  @Override
  public String next() {
    return currIterator.next();
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}
