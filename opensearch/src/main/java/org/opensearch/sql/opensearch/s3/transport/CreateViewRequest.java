/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.transport;

import java.io.IOException;
import java.util.Map;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class CreateViewRequest extends ActionRequest {

  private String tableName;

  private String indexName;

  private Map<String, Object> fieldsMapping;

  public CreateViewRequest() {
    super();
  }

  public CreateViewRequest(String tableName, String indexName, Map<String, Object> fieldsMapping) {
    super();
    this.tableName = tableName;
    this.indexName = indexName;
    this.fieldsMapping = fieldsMapping;
  }

  public CreateViewRequest(StreamInput in) throws IOException {
    super(in);
    this.tableName = in.readString();
    this.indexName = in.readString();
    this.fieldsMapping = in.readMap();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(tableName);
    out.writeString(indexName);
    out.writeMap(fieldsMapping);
  }

  @Override
  public ActionRequestValidationException validate() {
    // do nothing
    return null;
  }

  public String getIndexName() {
    return indexName;
  }

  public Map<String, Object> getFieldsMapping() {
    return fieldsMapping;
  }

  public String getTableName() {
    return tableName;
  }
}
