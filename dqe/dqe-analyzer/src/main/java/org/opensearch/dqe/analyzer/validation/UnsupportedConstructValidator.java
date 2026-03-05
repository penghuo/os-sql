/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.validation;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.With;
import java.util.Set;
import org.opensearch.dqe.parser.DqeUnsupportedOperationException;

/**
 * Visitor that walks a Trino Statement AST and rejects constructs not supported in Phase 1. Each
 * rejection throws {@link DqeUnsupportedOperationException} naming the specific unsupported
 * construct.
 *
 * <p>Rejected constructs: GROUP BY, HAVING, aggregate functions, JOIN (all types), subqueries
 * (scalar, IN, EXISTS), CTEs (WITH), UNION/INTERSECT/EXCEPT, window functions, MATCH_RECOGNIZE,
 * table functions, DISTINCT, SELECT-without-FROM, named function calls.
 */
public class UnsupportedConstructValidator {

  /** Well-known aggregate function names that are rejected in Phase 1. */
  private static final Set<String> AGGREGATE_FUNCTIONS =
      Set.of(
          "count",
          "sum",
          "avg",
          "min",
          "max",
          "array_agg",
          "collect_list",
          "collect_set",
          "stddev",
          "stddev_pop",
          "stddev_samp",
          "variance",
          "var_pop",
          "var_samp",
          "approx_distinct",
          "approx_percentile",
          "arbitrary",
          "every",
          "any_value",
          "bool_and",
          "bool_or",
          "corr",
          "covar_pop",
          "covar_samp",
          "regr_intercept",
          "regr_slope",
          "count_if",
          "histogram",
          "listagg",
          "string_agg");

  public UnsupportedConstructValidator() {}

  /**
   * Validates the statement, throwing on first unsupported construct found.
   *
   * @param statement the parsed Trino Statement
   * @throws DqeUnsupportedOperationException naming the unsupported construct
   */
  public void validate(Statement statement) {
    new Visitor().process(statement, null);
  }

  private static class Visitor extends AstVisitor<Void, Void> {

    @Override
    protected Void visitNode(Node node, Void context) {
      // Recursively visit all children
      for (Node child : node.getChildren()) {
        process(child, context);
      }
      return null;
    }

    @Override
    protected Void visitQuery(Query query, Void context) {
      if (query.getWith().isPresent()) {
        throw new DqeUnsupportedOperationException("WITH (Common Table Expressions)");
      }
      return super.visitQuery(query, context);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification qs, Void context) {
      // SELECT-without-FROM
      if (qs.getFrom().isEmpty()) {
        throw new DqeUnsupportedOperationException(
            "SELECT without FROM", "Phase 1 requires a FROM clause with a single table");
      }

      // DISTINCT
      if (qs.getSelect().isDistinct()) {
        throw new DqeUnsupportedOperationException("DISTINCT");
      }

      // GROUP BY
      if (qs.getGroupBy().isPresent()) {
        throw new DqeUnsupportedOperationException("GROUP BY");
      }

      // HAVING
      if (qs.getHaving().isPresent()) {
        throw new DqeUnsupportedOperationException("HAVING");
      }

      // Check SELECT items for window functions
      for (var selectItem : qs.getSelect().getSelectItems()) {
        if (selectItem instanceof SingleColumn sc) {
          process(sc.getExpression(), context);
        } else if (selectItem instanceof AllColumns) {
          // SELECT * is OK
        }
      }

      // Process FROM
      if (qs.getFrom().isPresent()) {
        process(qs.getFrom().get(), context);
      }

      // Process WHERE
      qs.getWhere().ifPresent(where -> process(where, context));

      // Process ORDER BY
      qs.getOrderBy()
          .ifPresent(
              orderBy -> orderBy.getSortItems().forEach(si -> process(si.getSortKey(), context)));

      return null;
    }

    @Override
    protected Void visitWith(With node, Void context) {
      throw new DqeUnsupportedOperationException("WITH (Common Table Expressions)");
    }

    @Override
    protected Void visitJoin(Join join, Void context) {
      throw new DqeUnsupportedOperationException("JOIN");
    }

    @Override
    protected Void visitUnion(Union node, Void context) {
      throw new DqeUnsupportedOperationException("UNION");
    }

    @Override
    protected Void visitIntersect(Intersect node, Void context) {
      throw new DqeUnsupportedOperationException("INTERSECT");
    }

    @Override
    protected Void visitExcept(Except node, Void context) {
      throw new DqeUnsupportedOperationException("EXCEPT");
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, Void context) {
      throw new DqeUnsupportedOperationException("subquery");
    }

    @Override
    protected Void visitExists(ExistsPredicate node, Void context) {
      throw new DqeUnsupportedOperationException("EXISTS subquery");
    }

    @Override
    protected Void visitGroupBy(GroupBy node, Void context) {
      throw new DqeUnsupportedOperationException("GROUP BY");
    }

    @Override
    protected Void visitFunctionCall(FunctionCall fc, Void context) {
      String name = fc.getName().toString().toLowerCase();

      // Check for aggregate functions
      if (AGGREGATE_FUNCTIONS.contains(name)) {
        throw new DqeUnsupportedOperationException("aggregate function '" + fc.getName() + "'");
      }

      // Check for window functions
      if (fc.getWindow().isPresent()) {
        throw new DqeUnsupportedOperationException("window function '" + fc.getName() + "'");
      }

      // All other named function calls are rejected in Phase 1
      throw new DqeUnsupportedOperationException(
          "function call '" + fc.getName() + "'",
          "named function calls are not supported in Phase 1");
    }

    @Override
    protected Void visitDelete(Delete node, Void context) {
      throw new DqeUnsupportedOperationException("DELETE");
    }

    @Override
    protected Void visitInsert(Insert node, Void context) {
      throw new DqeUnsupportedOperationException("INSERT");
    }

    @Override
    protected Void visitUpdate(Update node, Void context) {
      throw new DqeUnsupportedOperationException("UPDATE");
    }

    @Override
    protected Void visitCreateTable(CreateTable node, Void context) {
      throw new DqeUnsupportedOperationException("CREATE TABLE");
    }

    @Override
    protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Void context) {
      throw new DqeUnsupportedOperationException("CREATE TABLE AS SELECT");
    }

    @Override
    protected Void visitTable(Table node, Void context) {
      // Tables are fine — single-table FROM
      return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Void context) {
      // Aliased tables are fine
      return super.visitAliasedRelation(node, context);
    }
  }
}
