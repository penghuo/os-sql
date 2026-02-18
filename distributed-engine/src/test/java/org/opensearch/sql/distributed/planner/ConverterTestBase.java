/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for converter tests. Provides Calcite infrastructure to create synthetic RelNode trees
 * for testing the RelNodeToPlanNodeConverter.
 */
abstract class ConverterTestBase {

  protected RelNodeToPlanNodeConverter converter;
  protected RelOptCluster cluster;
  protected RexBuilder rexBuilder;
  protected RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    converter = new RelNodeToPlanNodeConverter();
    typeFactory = new JavaTypeFactoryImpl();
    rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    cluster = RelOptCluster.create(planner, rexBuilder);
  }

  /** Creates a LogicalTableScan with the given table name and column names (all VARCHAR). */
  protected TableScan createTableScan(String tableName, String... columns) {
    RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
    for (String col : columns) {
      typeBuilder.add(col, SqlTypeName.VARCHAR);
    }
    RelDataType rowType = typeBuilder.build();

    // Create a mock table
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(
        tableName,
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            return rowType;
          }
        });

    RelOptTable table =
        RelOptTableImpl.create(
            null, // RelOptSchema not needed for test
            rowType,
            List.of(tableName),
            null);

    return LogicalTableScan.create(cluster, table, List.of());
  }

  /** Creates a LogicalProject with the given projections and output column names. */
  protected LogicalProject createProject(
      RelNode input, List<RexNode> projects, String... outputNames) {
    RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
    for (String name : outputNames) {
      typeBuilder.add(name, SqlTypeName.VARCHAR);
    }
    RelDataType projectType = typeBuilder.build();
    return LogicalProject.create(input, List.of(), projects, projectType);
  }

  /** Creates a RexInputRef for the given field index. */
  protected RexInputRef fieldRef(int index) {
    return new RexInputRef(index, typeFactory.createSqlType(SqlTypeName.VARCHAR));
  }

  /** Creates a RexLiteral integer. */
  protected RexLiteral literal(int value) {
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
  }

  /** Creates a RexLiteral string. */
  protected RexLiteral literalString(String value) {
    return rexBuilder.makeLiteral(value);
  }

  /** Creates an exact integer literal for use in fetch/offset. */
  protected RexLiteral exactLiteral(int value) {
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
  }

  /** Creates a > comparison. */
  protected RexNode greaterThan(RexNode left, RexNode right) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, left, right);
  }

  /** Creates an = comparison. */
  protected RexNode equals(RexNode left, RexNode right) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
  }

  /** Creates a COUNT() AggregateCall with BIGINT return type. */
  protected AggregateCall countCall() {
    return AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        List.of(),
        -1,
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        "cnt");
  }
}
