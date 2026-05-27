/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/**
 * Spike: translate PPL UnresolvedPlan to a Calcite {@link SqlNode} tree.
 *
 * <p>Supports the {@code fields}/{@code table} command and the {@code head} command. Wildcard
 * expansion and field exclusion are resolved up-front against the table schema fetched from {@link
 * OpenSearchSchema}, so the produced {@link SqlSelect} is concrete enough to feed into the
 * standard {@code SqlValidator} / {@code SqlToRelConverter} pipeline.
 *
 * <p>Each PPL pipe produces a {@link SqlSelect}; pipes compose by nesting (the parent's {@code
 * FROM} is the child's {@link SqlSelect}). The visitor tracks the current set of fields available
 * after each pipe so subsequent pipes can resolve wildcards without help from the validator.
 */
public class PPLToSqlNodeVisitor extends AbstractNodeVisitor<SqlNode, PPLToSqlNodeVisitor.Frame> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /** Per-translation state: the most recent table identifier and current visible field list. */
  static final class Frame {
    SqlIdentifier sourceTable;
    List<String> currentFields;

    Frame(SqlIdentifier sourceTable, List<String> currentFields) {
      this.sourceTable = sourceTable;
      this.currentFields = currentFields;
    }
  }

  private final OpenSearchSchema schema;

  public PPLToSqlNodeVisitor(OpenSearchSchema schema) {
    this.schema = schema;
  }

  /** Entry point: returns a SqlSelect-shaped tree (or a SqlIdentifier if AST is bare Relation). */
  public SqlNode translate(org.opensearch.sql.ast.tree.UnresolvedPlan plan) {
    Frame frame = new Frame(null, null);
    return plan.accept(this, frame);
  }

  @Override
  public SqlNode visitRelation(Relation node, Frame frame) {
    List<String> parts = node.getTableQualifiedName().getParts();
    SqlIdentifier id = new SqlIdentifier(parts, POS);
    frame.sourceTable = id;
    frame.currentFields = lookupTableFields(id);
    return id;
  }

  @Override
  public SqlNode visitProject(Project node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);

    // AllFields: emit `*` and let SqlValidator expand against the FROM's row type.
    // frame.currentFields stays unchanged (passthrough).
    if (node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields) {
      return select(starList(), from, /* fetch */ null);
    }

    // After visiting child, frame.currentFields reflects the schema visible at this pipe.
    List<String> incomingFields = frame.currentFields;
    List<String> selected = resolveSelectedFields(node, incomingFields);

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
    SqlNodeList selectList = starList();
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getSize().toString(), POS);
    return select(selectList, from, fetch);
  }

  private SqlSelect select(SqlNodeList selectList, SqlNode from, SqlLiteral fetch) {
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

  private List<String> lookupTableFields(SqlIdentifier tableId) {
    String key = String.join(".", tableId.names);
    Table table = schema.getTableMap().get(key);
    if (table == null) {
      throw new IllegalStateException("Table not found in OpenSearchSchema: " + key);
    }
    return table.getRowType(org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY)
        .getFieldNames();
  }

  private List<String> resolveSelectedFields(Project node, List<String> incomingFields) {
    List<String> nonMeta =
        incomingFields.stream()
            .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
            .toList();

    List<String> requested = new ArrayList<>();
    Set<String> requestedSet = new LinkedHashSet<>();
    String firstWildcardWithNoMatch = null;
    for (UnresolvedExpression expr : node.getProjectList()) {
      if (!(expr instanceof Field field)) {
        throw new UnsupportedOperationException(
            "PPLToSqlNodeVisitor spike only supports plain Field projections, got "
                + expr.getClass().getSimpleName());
      }
      String name = field.getField().toString();
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

}
