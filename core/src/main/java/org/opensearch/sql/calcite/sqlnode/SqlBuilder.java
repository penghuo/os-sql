/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.calcite.sqlnode.PPLToSqlNodeVisitor.Frame;
import org.opensearch.sql.calcite.sqlnode.PPLToSqlNodeVisitor.JoinHints;

/**
 * DSL for the four SqlNode shapes {@link PPLToSqlNodeVisitor} produces: SELECT, JOIN, AS-alias, and
 * bare relation. Each builder centralises both the SqlNode construction AND the frame transition
 * that goes with it.
 *
 * <p>Two terminal styles encode different scope semantics:
 *
 * <ul>
 *   <li>{@code .wrap(frame)} — used by {@link SelectBuilder}. The wrapping SELECT seals off prior
 *       alias scope, so {@link Frame#joinHints} are cleared. Used when materialising the in-flight
 *       pipeline into a settled subquery.
 *   <li>{@code .build(frame)} — used by {@link JoinBuilder}, {@link AliasBuilder}, {@link
 *       RelationBuilder}. These leave the frame's join-disambiguation state alone (or set it, in
 *       the case of {@code joinHints(...)}). Used when the produced SqlNode is a participant in an
 *       enclosing scope, not a fresh one.
 * </ul>
 *
 * <p>Centralising both concerns prevents the bug where a new visitor mutates the SqlNode tree but
 * forgets the corresponding frame transition (or vice versa).
 */
final class SqlBuilder {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  private SqlBuilder() {}

  /** Start building a SELECT with the given (already-built) projection list. */
  static SelectBuilder select(SqlNodeList items) {
    return new SelectBuilder(items);
  }

  /** Start building a JOIN. */
  static JoinBuilder join() {
    return new JoinBuilder();
  }

  /**
   * Build {@code <inner> AS <alias>}. PPL's SubqueryAlias and join-side alias both desugar to this
   * shape. Returned SqlNode does not interact with frame state.
   */
  static SqlNode aliasAs(SqlNode inner, String alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(inner, new SqlIdentifier(alias, POS)), POS);
  }

  /** Build a bare table identifier and bind its field list onto the frame. */
  static SqlIdentifier relation(List<String> tableParts, List<String> fields, Frame frame) {
    SqlIdentifier id = new SqlIdentifier(tableParts, POS);
    frame.currentFields = fields;
    return id;
  }

  // ---------- SELECT ----------

  static final class SelectBuilder {
    private final SqlNodeList items;
    private SqlNode from;
    private SqlLiteral fetch;
    private List<String> newFields;

    private SelectBuilder(SqlNodeList items) {
      this.items = items;
    }

    SelectBuilder from(SqlNode from) {
      this.from = from;
      return this;
    }

    SelectBuilder fetch(SqlLiteral fetch) {
      this.fetch = fetch;
      return this;
    }

    /**
     * Optional. Sets the column list visible to the next pipe. Pass the wrapping select's output
     * field names; if omitted, {@link Frame#currentFields} is left unchanged.
     */
    SelectBuilder withFields(List<String> fields) {
      this.newFields = fields;
      return this;
    }

    /**
     * Terminal: build the {@link SqlSelect} and settle the frame.
     *
     * <p>Wrapping creates a new outer scope, so any join-disambiguation hints from a prior {@code
     * visitJoin} no longer apply (the inner aliases are now sealed inside the FROM subquery).
     * Cleared unconditionally.
     */
    SqlNode wrap(Frame frame) {
      if (newFields != null) {
        frame.currentFields = newFields;
      }
      frame.joinHints = null;
      return new SqlSelect(
          POS,
          /* keywordList */ SqlNodeList.EMPTY,
          items,
          from,
          /* where */ null,
          /* groupBy */ null,
          /* having */ null,
          /* windowDecls */ null,
          /* orderBy */ null,
          /* offset */ null,
          fetch,
          /* hints */ null);
    }
  }

  // ---------- JOIN ----------

  static final class JoinBuilder {
    private SqlNode left;
    private SqlNode right;
    private JoinType type = JoinType.INNER;
    private SqlNode condition;
    private JoinConditionType condType = JoinConditionType.NONE;
    private List<String> newFields;
    private JoinHints hints;

    private JoinBuilder() {}

    JoinBuilder left(SqlNode left) {
      this.left = left;
      return this;
    }

    JoinBuilder right(SqlNode right) {
      this.right = right;
      return this;
    }

    JoinBuilder type(JoinType type) {
      this.type = type;
      return this;
    }

    /** Set ON-clause condition. Use null for CROSS JOIN. */
    JoinBuilder on(SqlNode condition) {
      this.condition = condition;
      this.condType = condition == null ? JoinConditionType.NONE : JoinConditionType.ON;
      return this;
    }

    /**
     * Optional. Set the column list visible to the next pipe (used by the field-list-dedup path
     * when this join is wrapped in a SELECT downstream, or by the explicit-ON path to expose the
     * union of both sides' columns).
     */
    JoinBuilder withFields(List<String> fields) {
      this.newFields = fields;
      return this;
    }

    /**
     * Optional. Leave PPL's bind-bare-to-LEFT disambiguation state live on the frame so the next
     * pipe can resolve a bare {@code name} to {@code <leftAlias>.name} when it's ambiguous. Used by
     * the explicit-ON path; the field-list path doesn't need it because the dedup projection
     * already disambiguates.
     */
    JoinBuilder joinHints(String leftAlias, String rightAlias, Set<String> ambiguousColumns) {
      this.hints = new JoinHints(leftAlias, rightAlias, ambiguousColumns);
      return this;
    }

    /**
     * Terminal: build the {@link SqlJoin} and settle the frame. Does NOT clear scope (the join's
     * aliases stay live for the next pipe via the optional {@link #joinHints} call).
     */
    SqlNode build(Frame frame) {
      if (newFields != null) {
        frame.currentFields = newFields;
      }
      frame.joinHints = hints;
      return new SqlJoin(
          POS,
          left,
          SqlLiteral.createBoolean(false, POS),
          type.symbol(POS),
          right,
          condType.symbol(POS),
          condition);
    }
  }
}
