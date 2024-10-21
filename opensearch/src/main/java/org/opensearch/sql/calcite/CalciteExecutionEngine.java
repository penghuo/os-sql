/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;

public class CalciteExecutionEngine implements ExecutionEngine {

  private final NodeClient client;
  private final SchemaPlus rootSchema;
  private final Connection connection;
  private final CalciteAnalyzer analyzer;

  public CalciteExecutionEngine(OpenSearchClient openSearchClient) {
    this.client = openSearchClient.getNodeClient();
    try {
      this.connection= AccessController.doPrivileged(
          (PrivilegedAction<Connection>)
              () -> {
                try {
                  Class.forName("org.apache.calcite.jdbc.Driver");
                  return DriverManager.getConnection("jdbc:calcite:");
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
      this.rootSchema =
          connection.unwrap(CalciteConnection.class).getRootSchema();
      rootSchema.add("os",
          new OpenSearchSchema(client, new ObjectMapper(), null));

      FrameworkConfig frameworkConfig = AccessController.doPrivileged(
          (PrivilegedAction<FrameworkConfig>)
          () -> Frameworks.newConfigBuilder()
              .defaultSchema(rootSchema.getSubSchema("os"))
              .build()
      );
      RelBuilder builder = RelBuilder.create(frameworkConfig);
      this.analyzer = new CalciteAnalyzer(builder);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(UnresolvedPlan plan, ExecutionContext context,
                      ResponseListener<QueryResponse> listener) {

      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
          () -> {
            try {
              analyzer.analyze(plan, new AnalysisContext());
              RelNode relNode = analyzer.relBuilder.build();
              RelRunner runner = connection.unwrap(RelRunner.class);

              List<String> result = new ArrayList<>();
              try (ResultSet resultSet = runner.prepareStatement(relNode).executeQuery()) {
                new CalciteHelper.ResultSetFormatter().toStringList(resultSet, result);
              }
              Schema schema =
                  new Schema(ImmutableList.of(new Schema.Column("_MAP", "_MAP",
                      ExprCoreType.STRING)));
              QueryResponse queryResponse = new QueryResponse(schema,
                  result.stream().map(s -> ExprValueUtils.tupleValue(ImmutableMap.of("_MAP", s
                      ))).collect(Collectors.toList()), null);
              listener.onResponse(queryResponse);
              return null;
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
      );
  }


  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(PhysicalPlan plan, ExecutionContext context,
                      ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    throw new UnsupportedOperationException();
  }
}
