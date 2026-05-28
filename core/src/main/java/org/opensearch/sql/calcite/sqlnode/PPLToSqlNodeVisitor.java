/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/**
 * PPL AST → Calcite SqlNode translator. Compositional design: each visitor returns the SqlNode for
 * its subtree, and the parent visitor composes children's SqlNodes. Schema lookup goes through a
 * row-type provider keyed by table qualified name — no validator probe.
 *
 * <p>Status: POC, currently handles a subset of PPL commands needed for {@code
 * CalciteFieldsCommandIT} and the no-filter/no-eval/no-stats subset of {@code CalcitePPLJoinIT}.
 * Other commands throw {@code UnsupportedOperationException} until implemented. The legacy {@link
 * PplToSqlNode} remains in the source tree but is no longer wired into {@link
 * org.opensearch.sql.executor.QueryService} once this visitor takes over.
 */
public class PPLToSqlNodeVisitor extends AbstractNodeVisitor<SqlNode, PPLToSqlNodeVisitor.Frame> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /** Per-translation state: the visible field list and any active join-disambiguation hints. */
  static final class Frame {
    List<String> currentFields;

    /**
     * Set by an upstream {@code visitJoin} when the explicit-ON path leaves alias scope live for
     * the next pipe (so a downstream {@code | fields t1.col} can resolve). Cleared when a wrap
     * seals scope. {@code null} when no join hint applies. Stored as a single record so the
     * three-field invariant (all-or-none) is enforced by type, not by setter convention.
     */
    JoinHints joinHints;
  }

  /** Bind-bare-to-LEFT semantics state for the current join scope. */
  record JoinHints(String leftAlias, String rightAlias, Set<String> ambiguousColumns) {}

  /** Resolves a table qualified name (e.g. {@code ["my_index"]}) to its column names. */
  private final Function<List<String>, List<String>> tableFields;

  public PPLToSqlNodeVisitor(Function<List<String>, List<String>> tableFields) {
    this.tableFields = tableFields;
  }

  /** Public entry point. */
  public SqlNode translate(UnresolvedPlan plan) {
    Frame frame = new Frame();
    return stripImplicitMetaProjects(plan).accept(this, frame);
  }

  /**
   * Pre-pass: remove every {@code Project([AllFieldsExcludeMeta])} wrapper that AstBuilder inserted
   * around join sides, the top-level query, and subsearches. These wrappers exist only as a
   * v2-engine marker meaning "drop OpenSearch metadata fields from this subtree's output".
   *
   * <p>Materializing them as {@code SELECT cols FROM X} would hide the table identifier {@code X}
   * from the outer scope, breaking downstream {@code X.col} references. We strip them here and
   * apply the metadata filter post-conversion in {@link
   * org.opensearch.sql.executor.QueryService#analyze} via a RelNode-level shuttle.
   *
   * <p>User-written {@code | fields *} parses as {@code Project([AllFields])} (the parent class) so
   * it is NOT touched by this pass — only the AstBuilder-injected {@link AllFieldsExcludeMeta}
   * subclass marker is.
   */
  private static UnresolvedPlan stripImplicitMetaProjects(UnresolvedPlan plan) {
    if (plan instanceof Project p
        && !p.isExcluded()
        && p.getProjectList().size() == 1
        && p.getProjectList().getFirst() instanceof AllFieldsExcludeMeta
        && !p.getChild().isEmpty()) {
      return stripImplicitMetaProjects(p.getChild().get(0));
    }
    if (plan instanceof SubqueryAlias sa && !sa.getChild().isEmpty()) {
      UnresolvedPlan rewritten = stripImplicitMetaProjects(sa.getChild().get(0));
      if (rewritten != sa.getChild().get(0)) {
        return new SubqueryAlias(sa.getAlias(), rewritten);
      }
      return sa;
    }
    if (plan instanceof Join j) {
      UnresolvedPlan newLeft = stripImplicitMetaProjects(j.getChildren().get(0));
      UnresolvedPlan newRight = stripImplicitMetaProjects(j.getRight());
      if (newLeft != j.getChildren().get(0) || newRight != j.getRight()) {
        Join rebuilt =
            new Join(
                newRight,
                j.getLeftAlias(),
                j.getRightAlias(),
                j.getJoinType(),
                j.getJoinCondition(),
                j.getJoinHint(),
                j.getJoinFields(),
                j.getArgumentMap());
        rebuilt.attach(newLeft);
        return rebuilt;
      }
      return j;
    }
    if (plan instanceof Project p && !p.getChild().isEmpty()) {
      UnresolvedPlan rewritten = stripImplicitMetaProjects(p.getChild().get(0));
      if (rewritten != p.getChild().get(0)) {
        Project rebuilt = new Project(p.getProjectList(), p.getArgExprList());
        rebuilt.attach(rewritten);
        return rebuilt;
      }
      return p;
    }
    if (plan instanceof Head h && !h.getChild().isEmpty()) {
      UnresolvedPlan rewritten = stripImplicitMetaProjects(h.getChild().get(0));
      if (rewritten != h.getChild().get(0)) {
        return (UnresolvedPlan) h.attach(rewritten);
      }
      return h;
    }
    return plan;
  }

  @Override
  public SqlNode visitRelation(Relation node, Frame frame) {
    List<String> parts = node.getTableQualifiedName().getParts();
    return SqlBuilder.relation(parts, lookupTableFields(parts), frame);
  }

  @Override
  public SqlNode visitSubqueryAlias(SubqueryAlias node, Frame frame) {
    SqlNode inner = node.getChild().get(0).accept(this, frame);
    // PPL allows `source = X as i` and `[ source = X ] as i`; both produce a SubqueryAlias around
    // either a bare Relation or a sub-pipeline. Calcite needs the alias attached as a SqlNode
    // AS-call so qualified refs `i.col` resolve.
    return SqlBuilder.aliasAs(inner, node.getAlias());
  }

  @Override
  public SqlNode visitProject(Project node, Frame frame) {
    UnresolvedPlan childPlan = node.getChild().get(0);
    SqlNode from = childPlan.accept(this, frame);

    List<String> selected = resolveSelectedFields(node, frame.currentFields);

    // Build the SELECT list. Two responsibilities here:
    //
    //   1. Emit multi-part SqlIdentifiers for dotted user names (`t1.name` → `[t1, name]`).
    //      The validator resolves the parts itself — alias-qualified column vs STRUCT-field
    //      access is its job, not ours.
    //
    //   2. Disambiguate header labels for duplicate suffixes. Calcite labels a multi-part ref
    //      `t1.name` as just `name`; if the user wrote `| fields t1.name, t2.name`, both
    //      identifiers would label as `name` and Calcite auto-uniquifies the dup as `name0`.
    //      PPL's convention: the first occurrence keeps the bare suffix; subsequent occurrences
    //      keep their original dotted label. Wrap second+ dotted occurrences as `t2.name AS
    //      "t2.name"` (quoted so the dot survives in the resulting row-type field name).
    SqlNodeList selectList = new SqlNodeList(POS);
    Set<String> seenSuffixes = new LinkedHashSet<>();
    for (String name : selected) {
      SqlIdentifier ref = qualifyIfAmbiguous(name, frame);
      String suffix = name.substring(name.lastIndexOf('.') + 1);
      boolean firstSuffix = seenSuffixes.add(suffix);
      if (name.indexOf('.') >= 0 && !firstSuffix) {
        SqlIdentifier alias =
            new SqlIdentifier(
                java.util.Collections.singletonList(name),
                null,
                POS,
                List.of(SqlParserPos.ZERO.withQuoting(true)));
        selectList.add(new SqlBasicCall(SqlStdOperatorTable.AS, List.of(ref, alias), POS));
      } else {
        selectList.add(ref);
      }
    }

    return SqlBuilder.select(selectList)
        .from(from)
        .withFields(stripAliasPrefix(selected))
        .wrap(frame);
  }

  @Override
  public SqlNode visitHead(Head node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getSize().toString(), POS);
    return SqlBuilder.select(starList()).from(from).fetch(fetch).wrap(frame);
  }

  @Override
  public SqlNode visitJoin(Join node, Frame frame) {
    // Compose: walk the left side into the parent frame (so currentFields reflects the LEFT side
    // post-walk), then walk the right side in a fresh frame so its independent table/column scope
    // doesn't leak back. Build the SqlJoin from both sides plus the ON condition.
    SqlNode leftSide = node.getChildren().get(0).accept(this, frame);
    Frame rightFrame = new Frame();
    SqlNode rightSide = node.getRight().accept(this, rightFrame);

    // PPL allows referencing a side via either an explicit `left=l right=r` arg, or — when the
    // side is a SubqueryAlias — by the alias name. The visitor for SubqueryAlias already wrapped
    // the inner in `AS <alias>`. For the bare-table-without-explicit-alias case, attach a
    // default `__l` / `__r` only when we need the alias for synthetic ON conditions; otherwise
    // leave the bare table in scope (PPL `JOIN ON X.col = Y.col` references the raw table name).
    String leftAlias = node.getLeftAlias().orElse(null);
    String rightAlias = node.getRightAlias().orElse(null);

    // Settle the left/right alias if explicit. When the side is already AS-wrapped under a
    // different name (e.g. user wrote `source=X as tt | JOIN left=t1 ...`), the explicit join-arg
    // alias OVERRIDES the inner SubqueryAlias name — the outer scope sees only the new alias.
    leftSide = applyExplicitAlias(leftSide, leftAlias);
    rightSide = applyExplicitAlias(rightSide, rightAlias);

    JoinType jt = mapJoinType(node.getJoinType());

    // CROSS + condition: PPL allows `cross join ON ...`; SQL forbids it. Fall back to INNER.
    if (jt == JoinType.CROSS
        && (node.getJoinCondition().isPresent()
            || (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()))) {
      jt = JoinType.INNER;
    }

    SqlNode condition;
    if (node.getJoinCondition().isPresent()) {
      condition = exprForOn(node.getJoinCondition().get());
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      // PPL `join F1, F2 ...` — expand to `ON l.F = r.F AND ...` so qualified refs survive
      // (USING auto-dedupes columns and prevents qualified access; we want the explicit shape).
      String l = leftAliasOrDefault(leftAlias);
      String r = rightAliasOrDefault(rightAlias);
      // Need synthetic aliases on both sides to attach ON.l/r refs; force-alias if not yet.
      leftSide = ensureAliased(leftSide, leftAlias, "__l");
      rightSide = ensureAliased(rightSide, rightAlias, "__r");
      SqlNode acc = null;
      for (Field f : node.getJoinFields().get()) {
        String name = f.getField().toString();
        SqlNode lref = new SqlIdentifier(Arrays.asList(l, name), POS);
        SqlNode rref = new SqlIdentifier(Arrays.asList(r, name), POS);
        SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(lref, rref), POS);
        acc = (acc == null) ? eq : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, eq), POS);
      }
      condition = acc;
    } else if (jt != JoinType.CROSS) {
      // No ON, no field list, non-cross: PPL falls back to ON-equality on duplicate column names.
      List<String> shared = sharedFields(frame.currentFields, rightFrame.currentFields);
      if (!shared.isEmpty()) {
        String l = leftAliasOrDefault(leftAlias);
        String r = rightAliasOrDefault(rightAlias);
        leftSide = ensureAliased(leftSide, leftAlias, "__l");
        rightSide = ensureAliased(rightSide, rightAlias, "__r");
        SqlNode acc = null;
        for (String c : shared) {
          SqlNode lref = new SqlIdentifier(Arrays.asList(l, c), POS);
          SqlNode rref = new SqlIdentifier(Arrays.asList(r, c), POS);
          SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(lref, rref), POS);
          acc =
              (acc == null) ? eq : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, eq), POS);
        }
        condition = acc;
      } else {
        condition = SqlLiteral.createBoolean(true, POS);
      }
    } else {
      condition = null;
    }

    // Compute the post-join field list. For `JOIN F1, F2 ...` (and the auto-equality fallback)
    // PPL drops duplicates from one side (default overwrite=true → drop LEFT dups; keep RIGHT).
    boolean usesFieldList =
        node.getJoinFields().isPresent()
            && !node.getJoinFields().get().isEmpty()
            && !node.getJoinCondition().isPresent();
    boolean usesAutoEquality =
        !node.getJoinCondition().isPresent()
            && !(node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty())
            && jt != JoinType.CROSS;

    List<String> leftCols =
        frame.currentFields == null ? List.of() : stripMeta(frame.currentFields);
    List<String> rightCols =
        rightFrame.currentFields == null ? List.of() : stripMeta(rightFrame.currentFields);

    if (usesFieldList || usesAutoEquality) {
      // Field-list / auto-equality: wrap the SqlJoin in a SELECT that materialises the deduped
      // projection. A `| fields F1, F2` pipe after this join sees the deduped field list.
      // joinHints stays null on the frame — the explicit projection has already disambiguated.
      DedupedProjection dp = dedupedProjection(node, leftCols, rightCols, leftAlias, rightAlias);
      SqlNode join =
          SqlBuilder.join().left(leftSide).right(rightSide).type(jt).on(condition).build(frame);
      return SqlBuilder.select(dp.selectList).from(join).withFields(dp.names).wrap(frame);
    }

    // Explicit ON clause path. Expose the union of both sides' columns (so `t1.name`, `t2.name`
    // resolve in a downstream `| fields ...`) and leave PPL's bind-bare-to-LEFT hints live on
    // the frame so the next pipe can qualify ambiguous bare refs. Cleared by the next wrap.
    return SqlBuilder.join()
        .left(leftSide)
        .right(rightSide)
        .type(jt)
        .on(condition)
        .withFields(unionFieldsWithAliasPrefixes(leftCols, rightCols, leftAlias, rightAlias))
        .joinHints(leftAlias, rightAlias, new LinkedHashSet<>(sharedFields(leftCols, rightCols)))
        .build(frame);
  }

  /** Build the SELECT-list and column-name list for the field-list / auto-equality dedup path. */
  private DedupedProjection dedupedProjection(
      Join node,
      List<String> leftCols,
      List<String> rightCols,
      String leftAlias,
      String rightAlias) {
    boolean overwrite = true;
    if (node.getArgumentMap() != null && node.getArgumentMap().get("overwrite") != null) {
      Object v = node.getArgumentMap().get("overwrite").getValue();
      if (v instanceof Boolean b) overwrite = b;
    }
    String l = leftAliasOrDefault(leftAlias);
    String r = rightAliasOrDefault(rightAlias);
    Set<String> rightSet = new LinkedHashSet<>(rightCols);
    Set<String> leftSet = new LinkedHashSet<>(leftCols);
    List<String> dedupedNames = new ArrayList<>();
    SqlNodeList selectList = new SqlNodeList(POS);
    if (overwrite) {
      for (String c : leftCols) {
        if (rightSet.contains(c)) continue;
        selectList.add(new SqlIdentifier(Arrays.asList(l, c), POS));
        dedupedNames.add(c);
      }
      for (String c : rightCols) {
        selectList.add(new SqlIdentifier(Arrays.asList(r, c), POS));
        // RIGHT-side duplicates keep the bare name; LEFT-only references also keep the bare
        // name; user-facing column count is the union.
        dedupedNames.add(c);
      }
    } else {
      for (String c : leftCols) {
        selectList.add(new SqlIdentifier(Arrays.asList(l, c), POS));
        dedupedNames.add(c);
      }
      for (String c : rightCols) {
        if (leftSet.contains(c)) continue;
        selectList.add(new SqlIdentifier(Arrays.asList(r, c), POS));
        dedupedNames.add(c);
      }
    }
    return new DedupedProjection(selectList, dedupedNames);
  }

  private record DedupedProjection(SqlNodeList selectList, List<String> names) {}

  // ---------- Helpers ----------

  private static SqlNodeList starList() {
    SqlNodeList list = new SqlNodeList(POS);
    list.add(SqlIdentifier.star(POS));
    return list;
  }

  private List<String> lookupTableFields(List<String> tableParts) {
    List<String> fields = tableFields.apply(tableParts);
    if (fields == null) {
      throw new IllegalStateException("Table not found in catalog: " + tableParts);
    }
    return fields;
  }

  /**
   * Resolve the explicit SELECT-list field names this Project should emit, given the Project AST
   * and the field list visible at this pipe.
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li>If the projection is a single {@link AllFields}, return all non-metadata fields (the
   *       implicit final {@code | fields *} every PPL query carries).
   *   <li>Otherwise, expand wildcards and dedup.
   *   <li>If {@link Project#isExcluded()}, return {@code nonMeta - requested}; otherwise return
   *       {@code requested}.
   * </ul>
   */
  private List<String> resolveSelectedFields(Project node, List<String> incomingFields) {
    if (incomingFields == null) {
      // Post-join with explicit ON: the join layer didn't produce a deduped field list because
      // the user is expected to project explicitly. Tolerate by treating "all visible" as empty
      // for AllFields/wildcard purposes — explicit names will pass through.
      incomingFields = List.of();
    }
    List<String> nonMeta =
        incomingFields.stream()
            .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
            .toList();

    if (node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields) {
      return nonMeta;
    }

    List<String> requested = new ArrayList<>();
    Set<String> requestedSet = new LinkedHashSet<>();
    String firstWildcardWithNoMatch = null;
    for (UnresolvedExpression expr : node.getProjectList()) {
      String name = projectionFieldName(expr);
      if (WildcardUtils.containsWildcard(name)) {
        List<String> matches = WildcardUtils.expandWildcardPattern(name, nonMeta);
        if (matches.isEmpty() && firstWildcardWithNoMatch == null) {
          firstWildcardWithNoMatch = name;
        }
        for (String m : matches) {
          if (requestedSet.add(m)) {
            requested.add(m);
          }
        }
      } else if (requestedSet.add(name)) {
        requested.add(name);
      }
    }

    if (node.isExcluded()) {
      Set<String> exclude = new LinkedHashSet<>(requested);
      List<String> kept = nonMeta.stream().filter(f -> !exclude.contains(f)).toList();
      if (kept.isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid field exclusion: operation would exclude all fields from the result set");
      }
      return kept;
    }

    if (requested.isEmpty() && firstWildcardWithNoMatch != null) {
      throw new IllegalArgumentException(
          String.format("wildcard pattern [%s] matches no fields", firstWildcardWithNoMatch));
    }
    return requested;
  }

  private static String projectionFieldName(UnresolvedExpression expr) {
    if (expr instanceof Field field) {
      return field.getField().toString();
    }
    if (expr instanceof QualifiedName qn) {
      return qn.toString();
    }
    throw new UnsupportedOperationException(
        "Project supports only plain Field/QualifiedName projections at this stage, got "
            + expr.getClass().getSimpleName());
  }

  /**
   * Translate ON-clause expressions. Limited subset: literals, qualified names, comparisons,
   * AND/OR/NOT. Anything else throws — JoinIT exercises only this shape.
   */
  private SqlNode exprForOn(UnresolvedExpression e) {
    if (e instanceof Literal lit) {
      return literalToSqlNode(lit);
    }
    if (e instanceof QualifiedName qn) {
      return new SqlIdentifier(qn.getParts(), POS);
    }
    if (e instanceof Field f) {
      return exprForOn(f.getField());
    }
    if (e instanceof Compare c) {
      SqlNode l = exprForOn(c.getLeft());
      SqlNode r = exprForOn(c.getRight());
      return new SqlBasicCall(comparisonOperator(c.getOperator()), List.of(l, r), POS);
    }
    if (e instanceof And a) {
      return new SqlBasicCall(
          SqlStdOperatorTable.AND, List.of(exprForOn(a.getLeft()), exprForOn(a.getRight())), POS);
    }
    if (e instanceof Or o) {
      return new SqlBasicCall(
          SqlStdOperatorTable.OR, List.of(exprForOn(o.getLeft()), exprForOn(o.getRight())), POS);
    }
    if (e instanceof Not n) {
      return new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(exprForOn(n.getExpression())), POS);
    }
    throw new UnsupportedOperationException(
        "ON-clause expression not yet supported in PPLToSqlNodeVisitor: "
            + e.getClass().getSimpleName());
  }

  private static org.apache.calcite.sql.SqlOperator comparisonOperator(String op) {
    return switch (op) {
      case "=" -> SqlStdOperatorTable.EQUALS;
      case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
      case "<" -> SqlStdOperatorTable.LESS_THAN;
      case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case ">" -> SqlStdOperatorTable.GREATER_THAN;
      case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      default ->
          throw new UnsupportedOperationException("Comparison operator not supported: " + op);
    };
  }

  private static SqlNode literalToSqlNode(Literal lit) {
    Object v = lit.getValue();
    if (v == null) return SqlLiteral.createNull(POS);
    return switch (lit.getType()) {
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) v, POS);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(v.toString(), POS);
      case STRING -> SqlLiteral.createCharString(v.toString(), POS);
      default ->
          throw new UnsupportedOperationException(
              "Literal type not yet supported in ON-clause: " + lit.getType());
    };
  }

  private static JoinType mapJoinType(Join.JoinType jt) {
    return switch (jt) {
      case INNER -> JoinType.INNER;
      case LEFT -> JoinType.LEFT;
      case RIGHT -> JoinType.RIGHT;
      case FULL -> JoinType.FULL;
      case CROSS -> JoinType.CROSS;
      case SEMI, ANTI ->
          throw new UnsupportedOperationException(
              "SEMI/ANTI join not yet supported in PPLToSqlNodeVisitor");
    };
  }

  /** True when the SqlNode is already wrapped as `<inner> AS <alias>`. */
  private static boolean isAlreadyAliased(SqlNode side) {
    return side instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS;
  }

  /** Wrap {@code side} as {@code <side> AS <name>} when no AS-wrapper already exists. */
  private static SqlNode ensureAliased(SqlNode side, String explicitAlias, String defaultAlias) {
    if (isAlreadyAliased(side)) return side;
    String name = explicitAlias != null ? explicitAlias : defaultAlias;
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(side, new SqlIdentifier(name, POS)), POS);
  }

  private static String leftAliasOrDefault(String explicitAlias) {
    return explicitAlias != null ? explicitAlias : "__l";
  }

  private static String rightAliasOrDefault(String explicitAlias) {
    return explicitAlias != null ? explicitAlias : "__r";
  }

  /** Drop OpenSearch metadata fields from a column list. */
  private static List<String> stripMeta(List<String> fields) {
    return fields.stream()
        .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
        .toList();
  }

  /** Intersection of two non-meta column lists, preserving left-side order. */
  private static List<String> sharedFields(List<String> left, List<String> right) {
    if (left == null || right == null) return List.of();
    Set<String> rightSet =
        new LinkedHashSet<>(
            right.stream()
                .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
                .toList());
    List<String> shared = new ArrayList<>();
    for (String c : left) {
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
      if (rightSet.contains(c)) shared.add(c);
    }
    return shared;
  }

  /**
   * Build the post-join visible field list for the explicit-ON path. Returns the union of left and
   * right column names; for columns that exist on both sides we ALSO add alias-qualified entries
   * ({@code l.col}, {@code r.col}) so a downstream `| fields t1.name, t2.name` can resolve them.
   * Order: left bare cols, right bare cols (deduped), alias-qualified cols.
   */
  private static List<String> unionFieldsWithAliasPrefixes(
      List<String> left, List<String> right, String leftAlias, String rightAlias) {
    Set<String> seen = new LinkedHashSet<>();
    List<String> out = new ArrayList<>();
    for (String c : left) {
      if (seen.add(c)) out.add(c);
    }
    for (String c : right) {
      if (seen.add(c)) out.add(c);
    }
    // Alias-qualified: for any column name present in either list, expose `<alias>.<col>` under
    // its explicit alias. This is for the `| fields t1.name, t2.name` shape; the validator's
    // multi-part identifier resolution handles the actual binding so we just need to list the
    // names so the project resolver doesn't reject them as unknown.
    if (leftAlias != null) {
      for (String c : left) {
        String q = leftAlias + "." + c;
        if (seen.add(q)) out.add(q);
      }
    }
    if (rightAlias != null) {
      for (String c : right) {
        String q = rightAlias + "." + c;
        if (seen.add(q)) out.add(q);
      }
    }
    return out;
  }

  /**
   * Strip the {@code <alias>.} prefix from each element so frame.currentFields stays in
   * "user-facing column name" form for downstream pipes. Names without a dot pass through; names
   * with a dot keep only the suffix.
   */
  private static List<String> stripAliasPrefix(List<String> names) {
    List<String> out = new ArrayList<>(names.size());
    Set<String> seen = new LinkedHashSet<>();
    for (String n : names) {
      int dot = n.indexOf('.');
      String stripped = dot < 0 ? n : n.substring(dot + 1);
      if (seen.add(stripped)) out.add(stripped);
    }
    return out;
  }

  /** Convert a possibly-dotted field name into a {@link SqlIdentifier} with multiple parts. */
  private static SqlIdentifier toIdentifier(String name) {
    if (name.indexOf('.') < 0) return new SqlIdentifier(name, POS);
    return new SqlIdentifier(Arrays.asList(name.split("\\.")), POS);
  }

  /**
   * If {@code name} is a bare column that exists on BOTH sides of a recent join, prefix it with the
   * left alias so Calcite resolves it without raising "Column 'x' is ambiguous". PPL binds bare
   * references to the LEFT side. Names that are already qualified ({@code a.col}) pass through
   * unchanged.
   */
  private static SqlIdentifier qualifyIfAmbiguous(String name, Frame frame) {
    JoinHints hints = frame.joinHints;
    if (name.indexOf('.') < 0
        && hints != null
        && hints.leftAlias() != null
        && hints.ambiguousColumns().contains(name)) {
      return new SqlIdentifier(Arrays.asList(hints.leftAlias(), name), POS);
    }
    return toIdentifier(name);
  }

  /**
   * Apply an explicit join-arg alias to a join side. When {@code explicitAlias} is null, the side
   * keeps whatever alias (or none) it already carries. When {@code explicitAlias} is non-null, the
   * side is wrapped as {@code <inner> AS <alias>} — replacing any existing AS-wrapper. PPL
   * semantics: an explicit {@code left=t1}/{@code right=t2} on the JOIN command overrides any inner
   * SubqueryAlias for the join's outer scope.
   */
  private static SqlNode applyExplicitAlias(SqlNode side, String explicitAlias) {
    if (explicitAlias == null) return side;
    SqlNode inner =
        side instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS
            ? sbc.operand(0)
            : side;
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(inner, new SqlIdentifier(explicitAlias, POS)), POS);
  }
}
