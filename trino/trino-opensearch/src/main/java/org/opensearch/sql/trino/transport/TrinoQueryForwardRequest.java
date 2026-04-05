/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Request to forward a SQL query to another node for execution. */
public class TrinoQueryForwardRequest extends ActionRequest {

  private final String sql;
  private final String catalog;
  private final String schema;

  public TrinoQueryForwardRequest(String sql, String catalog, String schema) {
    this.sql = sql;
    this.catalog = catalog;
    this.schema = schema;
  }

  public TrinoQueryForwardRequest(StreamInput in) throws IOException {
    super(in);
    this.sql = in.readString();
    this.catalog = in.readString();
    this.schema = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(sql);
    out.writeString(catalog);
    out.writeString(schema);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getSql() {
    return sql;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }
}
