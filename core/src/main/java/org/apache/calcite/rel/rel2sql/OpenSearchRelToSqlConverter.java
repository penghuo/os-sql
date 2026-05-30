/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.calcite.rel.rel2sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Custom {@link RelToSqlConverter} that fixes SEMI/ANTI Join unparse for the case where the left
 * and right inputs share a column name AND the left side is a simple (non-Join) input.
 *
 * <p>Calcite's default {@code visitAntiOrSemiJoin}:
 *
 * <ol>
 *   <li>Builds {@code WHERE EXISTS (SELECT 1 FROM right WHERE leftCol = rightAlias.rightCol)}.
 *   <li>Runs an {@code AliasReplacementShuttle} that REPLACES {@code tableAlias.fieldName} in the
 *       condition with the corresponding SELECT-list item from the OUTER select. Typical outer
 *       SELECT lists use unqualified column names, so this STRIPS the qualifier.
 * </ol>
 *
 * <p>When both tables have a column named {@code name}, the unqualified left-side reference inside
 * {@code EXISTS (... WHERE name = t2.name)} is resolved by the validator to the INNER table's
 * column, collapsing the join condition to {@code t2.name = t2.name} (always true). SEMI then
 * degenerates to a star-cross-join filter and ANTI returns no rows.
 *
 * <p>This subclass skips the {@code AliasReplacementShuttle} so the qualified condition (e.g.
 * {@code t1.name = t2.name}) survives into the generated SQL. The validator can then resolve
 * each side correctly.
 *
 * <p><b>Complex-LEFT fallback:</b> when the LEFT side is itself a Join (multi-join shape), the
 * fix produces SQL of the form {@code (JOIN) t11 ... WHERE t11.col = ...} which the Babel parser
 * rejects with "Join expression encountered in illegal context" (it does not accept a parenthesised
 * Join as a TableRef). For those shapes, fall back to the default Calcite behaviour, which uses
 * the {@code AliasReplacementShuttle} to strip the qualifier — that lets {@code stripUnusedAsOverJoin}
 * remove the now-unused alias and unwrap the JOIN, sidestepping the parser limitation. The trade-off
 * is the same-name collision can still appear in multi-join SEMI/ANTI tests, but those test cases
 * are rare in practice; the simple-LEFT fix covers the common case.
 *
 * <p>Lives in {@code org.apache.calcite.rel.rel2sql} to access package-private members of {@link
 * RelToSqlConverter}, {@link SqlImplementor} and {@link SqlImplementor.Result}.
 */
public final class OpenSearchRelToSqlConverter extends RelToSqlConverter {

  public OpenSearchRelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  @Override
  protected Result visitAntiOrSemiJoin(Join e) {
    if (containsJoinDeep(e.getLeft())) {
      // Defer to the default behaviour for complex-LEFT cases (see class doc).
      return super.visitAntiOrSemiJoin(e);
    }
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();

    final SqlSelect sqlSelect = leftResult.asSelect();
    SqlNode sqlCondition =
        convertConditionToSqlNode(
            e.getCondition(),
            leftResult.qualifiedContext(),
            rightResult.qualifiedContext());

    // INTENTIONALLY skip the default AliasReplacementShuttle — see class doc.

    SqlNode fromPart = rightResult.asFrom();
    SqlSelect existsSqlSelect;
    if (fromPart.getKind() == SqlKind.SELECT) {
      existsSqlSelect = (SqlSelect) fromPart;
      existsSqlSelect.setSelectList(new SqlNodeList(ImmutableList.of(ONE), SqlParserPos.ZERO));
      if (existsSqlSelect.getWhere() != null) {
        sqlCondition =
            SqlStdOperatorTable.AND.createCall(
                SqlParserPos.ZERO, existsSqlSelect.getWhere(), sqlCondition);
      }
      existsSqlSelect.setWhere(sqlCondition);
    } else {
      existsSqlSelect =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              new SqlNodeList(ImmutableList.of(ONE), SqlParserPos.ZERO),
              fromPart,
              sqlCondition,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
    }
    sqlCondition = SqlStdOperatorTable.EXISTS.createCall(SqlParserPos.ZERO, existsSqlSelect);
    if (e.getJoinType() == JoinRelType.ANTI) {
      sqlCondition = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, sqlCondition);
    }
    if (sqlSelect.getWhere() != null) {
      sqlCondition =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO, sqlSelect.getWhere(), sqlCondition);
    }
    sqlSelect.setWhere(sqlCondition);

    if (leftResult.neededAlias != null && sqlSelect.getFrom() != null) {
      sqlSelect.setFrom(as(sqlSelect.getFrom(), leftResult.neededAlias));
    }
    return result(sqlSelect, ImmutableList.of(Clause.FROM), e, null);
  }

  /** True if the plan rooted at {@code root} contains a {@link Join} anywhere in the tree. */
  private static boolean containsJoinDeep(RelNode root) {
    boolean[] found = {false};
    root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            if (node instanceof Join) {
              found[0] = true;
              return node;
            }
            return super.visit(node);
          }
        });
    return found[0];
  }
}
