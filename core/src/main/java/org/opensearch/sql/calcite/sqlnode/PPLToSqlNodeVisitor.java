/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/**
 * PPL AST → Calcite SqlNode translator. Compositional design: each visitor returns the SqlNode for
 * its subtree, and the parent visitor composes children's SqlNodes. Schema lookup goes through a
 * row-type provider keyed by table qualified name — no validator probe.
 *
 * <p>Status: POC, currently handles a subset of PPL commands needed for {@code
 * CalciteFieldsCommandIT}. Other commands throw {@code UnsupportedOperationException} until
 * implemented. The legacy {@link PplToSqlNode} remains in the source tree but is no longer wired
 * into {@link org.opensearch.sql.executor.QueryService} once this visitor takes over.
 */
public class PPLToSqlNodeVisitor extends AbstractNodeVisitor<SqlNode, PPLToSqlNodeVisitor.Frame> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /** Per-translation state: the table identifier seen so far and the visible field list. */
  static final class Frame {
    SqlIdentifier sourceTable;
    List<String> currentFields;

    Frame(SqlIdentifier sourceTable, List<String> currentFields) {
      this.sourceTable = sourceTable;
      this.currentFields = currentFields;
    }
  }

  /** Resolves a table qualified name (e.g. {@code ["my_index"]}) to its column names. */
  private final Function<List<String>, List<String>> tableFields;

  public PPLToSqlNodeVisitor(Function<List<String>, List<String>> tableFields) {
    this.tableFields = tableFields;
  }

  /** Public entry point. */
  public SqlNode translate(UnresolvedPlan plan) {
    Frame frame = new Frame(null, null);
    return plan.accept(this, frame);
  }

  @Override
  public SqlNode visitRelation(Relation node, Frame frame) {
    QualifiedName qn = node.getTableQualifiedName();
    SqlIdentifier id = new SqlIdentifier(qn.getParts(), POS);
    frame.sourceTable = id;
    frame.currentFields = lookupTableFields(qn.getParts());
    return id;
  }

  @Override
  public SqlNode visitProject(Project node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);

    List<String> selected = resolveSelectedFields(node, frame.currentFields);

    SqlNodeList selectList = new SqlNodeList(POS);
    for (String name : selected) {
      selectList.add(new SqlIdentifier(name, POS));
    }

    frame.currentFields = selected;
    return select(selectList, from, /* fetch */ null);
  }

  @Override
  public SqlNode visitHead(Head node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getSize().toString(), POS);
    return select(starList(), from, fetch);
  }

  // ---------- Helpers ----------

  private static SqlSelect select(SqlNodeList selectList, SqlNode from, SqlLiteral fetch) {
    return new SqlSelect(
        POS,
        /* keywordList */ SqlNodeList.EMPTY,
        selectList,
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
}
