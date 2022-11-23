/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.statement.ddl.Column;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.streaming.MicroBatchStreamingExecution;
import org.opensearch.sql.storage.MetaStore;

public class DataDefinitionPlan extends AbstractPlan {

  private static final Logger log = LogManager.getLogger();

  // todo. need refactor. QualifiedName should be resolved in analysis stage.
  private final QualifiedName tableName;

  private final List<Column> columns;

  private final String fileFormat;

  private final String location;

  private final ResponseListener<ExecutionEngine.QueryResponse> listener;

  private final MetaStore metaStore;

  public DataDefinitionPlan(
      QueryId queryId,
      QualifiedName tableName,
      List<Column> columns,
      String fileFormat,
      String location,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId);
    this.tableName = tableName;
    this.columns = columns;
    this.fileFormat = fileFormat;
    this.location = location;
    this.metaStore = MetaStore.instance();
    this.listener = listener;
  }

  @Override
  public void execute() {
    try {
      List<String> parts = tableName.getParts();
      this.metaStore.add(
          new MetaStore.TableMetaData(parts.get(parts.size() - 1), columns, fileFormat, location));
      listener.onResponse(
          new ExecutionEngine.QueryResponse(
              new ExecutionEngine.Schema(
                  ImmutableList.of(new ExecutionEngine.Schema.Column("status", "status", STRING))),
              ImmutableList.of(ExprValueUtils.tupleValue(ImmutableMap.of("status", "success")))));
    } catch (Exception e) {
      log.error(e);
      listener.onFailure(e);
    }
  }

  @Override
  public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    // todo
    throw new UnsupportedOperationException();
  }
}
