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

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * S3Scan Todo.
 */
public class S3Scan implements Iterator<ExprValue> {

  private static final Logger log = LogManager.getLogger(S3Scan.class);

  private final Iterator<Pair<String, String>> s3Objects;

  private S3ObjectContent content;

  public S3Scan(List<Pair<String, String>> s3Objects) {
    this.s3Objects = s3Objects.iterator();
    content = new S3ObjectContent();
  }

  public void open() {
    final Pair<String, String> next = s3Objects.next();
    System.out.println("next file " + next);
    content.open(next);
  }

  // either content not been consumed or s3 objects still not been consumed.
  @Override
  public boolean hasNext() {
    if (content.hasNext()) {
      return true;
    } else if (!s3Objects.hasNext()) {
      return false;
    } else {
      content.close();
      final Pair<String, String> next = s3Objects.next();
      System.out.println("next file " + next);
      content.open(next);
      return content.hasNext();
    }
  }

  @Override
  public ExprValue next() {
    return asExprValue(content.next());
  }

  private ExprValue asExprValue(String line) {
    return ExprTupleValue.fromExprValueMap(ImmutableMap.of("raw", new ExprStringValue(line)));
  }
}