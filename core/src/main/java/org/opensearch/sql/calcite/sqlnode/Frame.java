/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.expression.DataType;

/**
 * Per-translation state for {@link PPLToSqlNodeVisitor}: the visible field list and any active
 * pipe-level disambiguation hints that need to cross pipe boundaries.
 *
 * <p>Each {@code visit*} call receives a Frame and may mutate it; siblings in a tree (left/right
 * sides of a JOIN) get fresh frames so their independent column scopes don't bleed.
 */
final class Frame {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  List<String> currentFields;

  /**
   * Alias-name synonyms: when AstBuilder injects {@code SubqueryAlias("outer", SubqueryAlias(
   * "inner", X))} for a side that has both an inner {@code as inner} AND an explicit {@code
   * left=outer} or {@code right=outer} join arg, the outer name overrides for the validator while
   * {@code inner} is recorded here so a downstream {@code | fields inner.col} still resolves under
   * {@code outer.col}. Empty when no synonyms are active.
   */
  Map<String, String> aliasSynonyms = new LinkedHashMap<>();

  /**
   * Set by an upstream {@code visitJoin} when the explicit-ON path leaves alias scope live for the
   * next pipe (so a downstream {@code | fields t1.col} can resolve). Cleared when a wrap seals
   * scope. {@code null} when no join hint applies. Stored as a single record so the three-field
   * invariant (all-or-none) is enforced by type, not by setter convention.
   */
  JoinHints joinHints;

  /**
   * Most-recent sort keys seen anywhere in the pipeline. Used by {@code visitReverse} to flip the
   * active ordering. Set by {@link SqlBuilder.SelectBuilder#orderBy} via {@code wrap}. Survives a
   * wrap (the keys themselves remain semantically valid even when the SqlSelect changes scope) —
   * only cleared by visitors that destroy row-level collation (e.g. visitAggregation).
   */
  List<SqlNode> lastOrderBy;

  /**
   * True when the most-recent emitted order-by came from {@code visitReverse}'s implicit {@code
   * @timestamp DESC} fallback (case 2: no prior sort but @timestamp visible). PPL semantics: a
   * downstream user-explicit {@code | sort F} OVERRIDES this implicit reverse sort (the user is
   * asking for a different ordering, not stacking). When set, {@code visitSort} should drop the
   * inner reverse-implicit sort.
   */
  boolean reverseImplicitOrderBy;

  /**
   * Eval-alias → statically-known {@link DataType} map. Populated by {@code visitEval} when a let's
   * RHS has a derivable static type ({@code Cast(... AS DOUBLE)}, {@code Literal(STRING)}, {@code
   * Function("date", ...)}, etc.). Consumed by {@code castExpr} to dispatch {@code
   * NUMBER_TO_STRING} / IP_TO_STRING / etc. for column refs whose type can be resolved without a
   * validator probe. Cleared on aggregation / pipe boundaries that wipe collation.
   */
  Map<String, DataType> evalAliasTypes = new LinkedHashMap<>();

  /**
   * Eval-alias → source-column name when the let RHS is a bare passthrough column reference ({@code
   * eval name = lastname}). Populated by {@code visitEval} only for trivial passthroughs (bare
   * {@code QualifiedName}); excluded for any computed RHS. Consumed by {@code visitAggregation}'s
   * doc_count optimization to substitute the alias back to its source for the IS NOT NULL filter —
   * mirrors v2's RelBuilder which preserves the original column index across rename-only Project
   * layers.
   */
  Map<String, String> evalPassthroughSource = new LinkedHashMap<>();

  /**
   * Set of catalog column names whose Calcite type is ARRAY — these correspond to OpenSearch {@code
   * type: nested} mappings. Populated during {@code visitRelation}. Consumed to detect aggregation
   * over dotted fields whose ROOT is array-typed (true nested) — those queries are unsupported in
   * the SqlNode pipeline and must fail with PPL's documented error message.
   */
  Set<String> arrayRootedFields = new LinkedHashSet<>();

  /**
   * Per-column UDT name when the underlying catalog column has a PPL user-defined type ({@code
   * EXPR_TIMESTAMP}, {@code EXPR_DATE}, {@code EXPR_TIME}, {@code EXPR_IP}). Populated by {@code
   * visitRelation} from the catalog row type and propagated through pipes that preserve column
   * identity (eval, fields, sort, head, ...). Consumed by {@code visitAppend} / {@code
   * visitMultisearch} / {@code visitUnion} so absent UDT columns get a typed-NULL pad ({@code
   * TIMESTAMP(CAST(NULL AS VARCHAR))} for {@code EXPR_TIMESTAMP}, etc.) instead of a raw {@code
   * NULL} that collapses the UDT into VARCHAR through least-restrictive type computation. Maps
   * column name → lowercase UDT root ({@code timestamp}, {@code date}, {@code time}, {@code ip}).
   * Empty when no UDT columns are visible.
   */
  Map<String, String> columnUdt = new LinkedHashMap<>();

  /**
   * Per-column primitive {@link DataType} for catalog columns whose Calcite type maps cleanly to a
   * PPL primitive (BOOLEAN, INTEGER, LONG, DOUBLE, etc.). Populated by {@code visitRelation} from
   * the catalog row type. Used by {@code castExpr} to detect source-type conditions (e.g.
   * BOOLEAN→INT cast needs CASE-WHEN expansion since Calcite SAFE_CAST rejects it). Does NOT cover
   * the UDT types ({@link #columnUdt} handles those).
   */
  Map<String, DataType> columnPrimitiveType = new LinkedHashMap<>();

  /**
   * Per-column PPL legacy type name for catalog columns that need a sub-type distinction beyond
   * what {@link DataType} captures (e.g. TINYINT/BYTE vs SMALLINT/SHORT both map to {@link
   * DataType#SHORT}, but PPL's {@code typeof()} differentiates). Populated by {@code visitRelation}
   * from the catalog Calcite SqlTypeName. Consumed by {@code pplLegacyTypeName} for {@code
   * typeof(<column>)} dispatch.
   */
  Map<String, String> columnLegacyTypeName = new LinkedHashMap<>();

  /**
   * Top-level catalog columns whose Calcite type is MAP&lt;VARCHAR, ANY&gt;. PPL converts
   * OpenSearch "object" mappings to MAP. When a dotted identifier like {@code object_value.first}
   * starts at one of these columns, emit {@code ITEM(object_value, 'first')} so the validator
   * drills into the map instead of interpreting the leading part as a table name. Skipped for
   * ARRAY-typed (nested) columns since ITEM rejects string keys for arrays.
   */
  Set<String> mapColumns = new LinkedHashSet<>();

  /**
   * Eval aliases whose name contains a literal dot (e.g. {@code spath input=doc} produces {@code
   * doc.user.name} as an alias). When a downstream expression references these by their dotted
   * name, emit a quoted single-part identifier so the validator looks up the literal column instead
   * of treating dot parts as schema/table/column.
   */
  Set<String> dottedEvalAliases = new LinkedHashSet<>();

  /**
   * Names of join-arg aliases ({@code left=a, right=b}) live in this pipeline's history. Used by
   * {@code toIdentifier} to gate the dotted-flat-column rewrite — when the leading part is a known
   * join alias, keep the multi-part identifier so {@code a.name} resolves through alias scope.
   * Carries across pipe wraps.
   */
  Set<String> liveJoinAliases = new LinkedHashSet<>();

  /**
   * Active SubqueryAlias name set by {@code visitSubqueryAlias} from {@code source = X as i}. Used
   * by visitors that wrap state (visitFilter / visitSort / visitEval / visitAggregation / etc.) to
   * re-attach the alias around the wrapped subquery so a downstream {@code | sort i.col} still
   * resolves the alias. Cleared by {@code visitJoin} once the alias is consumed via
   * applyExplicitAlias.
   */
  String subqueryAliasName;

  /**
   * Return the most-recent sort keys with each direction flipped (ASC ↔ DESC, NULLS_FIRST ↔
   * NULLS_LAST). Returns {@code null} if no prior sort exists. Used by {@code visitReverse}.
   */
  List<SqlNode> reversedLastOrderBy() {
    return reverseSortKeys(lastOrderBy);
  }

  /**
   * Flip each sort key (ASC ↔ DESC, NULLS_FIRST ↔ NULLS_LAST). Recognises the {@code
   * NULLS_FIRST/LAST(DESC?(expr))} shape produced by visitor-side sort-key builders.
   */
  private static List<SqlNode> reverseSortKeys(List<SqlNode> keys) {
    if (keys == null || keys.isEmpty()) return null;
    List<SqlNode> out = new ArrayList<>(keys.size());
    for (SqlNode k : keys) {
      if (k instanceof SqlBasicCall outer
          && (outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
              || outer.getOperator() == SqlStdOperatorTable.NULLS_LAST)) {
        org.apache.calcite.sql.SqlOperator flippedNulls =
            outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
                ? SqlStdOperatorTable.NULLS_LAST
                : SqlStdOperatorTable.NULLS_FIRST;
        SqlNode inner = outer.operand(0);
        SqlNode flippedInner;
        if (inner instanceof SqlBasicCall innerCall
            && innerCall.getOperator() == SqlStdOperatorTable.DESC) {
          flippedInner = innerCall.operand(0);
        } else {
          flippedInner = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(inner), POS);
        }
        out.add(new SqlBasicCall(flippedNulls, List.of(flippedInner), POS));
      } else {
        out.add(k);
      }
    }
    return out;
  }
}
