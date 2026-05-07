/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;

/**
 * POC: translate a PPL {@link UnresolvedPlan} pipeline into a Calcite {@link SqlNode} tree.
 *
 * <p>Compilation strategy: accumulate consecutive pipes into a single {@link SqlSelect}, wrapping
 * the in-flight select as a subquery only when the next pipe would violate SQL semantics —
 * specifically:
 *
 * <ul>
 *   <li>WHERE after eval-extended projection (condition might reference aliases from same SELECT
 *       list; SQL only allows that in HAVING).
 *   <li>EVAL/PROJECT after ORDER BY or FETCH (extending the row set past a row-cap is wrong).
 *   <li>PROJECT after EVAL extended the projection (SELECT list aliases not visible in same list).
 *   <li>SORT/HEAD after FETCH already set.
 * </ul>
 *
 * <p>Currently handles {@code source} / {@code where} / {@code eval} / {@code fields} / {@code
 * sort} / {@code head} / {@code limit}; other commands fall through to {@link
 * UnsupportedOperationException}.
 */
public class PplToSqlNode {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /**
   * Optional row-type oracle. Some PPL commands (flatten, lookup REPLACE, rename) need to know the
   * columns of the input row type at translation time to emit the right projection. The caller
   * (typically {@link SqlNodePlanner}) supplies a function that runs the partially-built {@link
   * SqlNode} through Calcite's validator and returns its row type. Without an oracle, those
   * commands raise {@link UnsupportedOperationException}.
   */
  private final java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType>
      rowTypeOracle;

  public PplToSqlNode() {
    this(null);
  }

  public PplToSqlNode(
      java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType> rowTypeOracle) {
    this.rowTypeOracle = rowTypeOracle;
  }

  private List<String> deriveColumnNames(SqlNode partialFrom) {
    if (rowTypeOracle == null) {
      throw new UnsupportedOperationException(
          "This PPL pipe needs schema introspection (flatten / rename / lookup REPLACE / etc.);"
              + " supply a row-type oracle to PplToSqlNode to enable it.");
    }
    // Wrap the partial FROM in a SELECT * to make it a queryable expression.
    SqlSelect probe =
        new SqlSelect(
            POS, /* keywordList */
            null,
            starList(),
            partialFrom, /* where */
            null,
            /* group */ null, /* having */
            null, /* windowList */
            null,
            /* qualify */ null, /* orderBy */
            null, /* offset */
            null, /* fetch */
            null,
            /* hints */ null);
    org.apache.calcite.rel.type.RelDataType rt = rowTypeOracle.apply(probe);
    return rt.getFieldList().stream()
        .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
        .toList();
  }

  /** Public entry point. */
  public SqlNode visit(UnresolvedPlan plan) {
    Pipeline state = new Pipeline();
    new Builder(state).walk(plan);
    return state.toFinalSqlNode();
  }

  /** Walks a PPL pipeline (linked through {@code child}) bottom-up. */
  private final class Builder extends AbstractNodeVisitor<Void, Void> {
    private final Pipeline state;

    Builder(Pipeline state) {
      this.state = state;
    }

    void walk(UnresolvedPlan plan) {
      plan.accept(this, null);
    }

    private void walkChild(UnresolvedPlan plan) {
      if (!plan.getChild().isEmpty()) {
        plan.getChild().get(0).accept(this, null);
      }
    }

    @Override
    public Void visitRelation(Relation node, Void ignored) {
      // PPL `source=a, b` → SQL UNION ALL of two SELECT * statements, identical to how the
      // existing CalciteRelNodeVisitor materializes multiple tables.
      List<UnresolvedExpression> tables = node.getTableNames();
      if (tables.size() == 1) {
        state.setFrom(qualifiedNameToIdentifier((QualifiedName) tables.get(0)));
      } else {
        SqlNode union = null;
        for (UnresolvedExpression t : tables) {
          SqlNode select =
              new SqlSelect(
                  POS,
                  /* keywordList */ null,
                  starList(),
                  qualifiedNameToIdentifier((QualifiedName) t),
                  /* where */ null,
                  /* group */ null,
                  /* having */ null,
                  /* windowList */ null,
                  /* qualify */ null,
                  /* orderBy */ null,
                  /* offset */ null,
                  /* fetch */ null,
                  /* hints */ null);
          union =
              union == null
                  ? select
                  : new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, select), POS);
        }
        state.setFrom(union);
      }
      return null;
    }

    @Override
    public Void visitFilter(Filter node, Void ignored) {
      walkChild(node);
      // WHERE evaluates before SELECT; aliases introduced by upstream Eval/Project aren't legal
      // in a same-level WHERE. Wrap if projection has already been customised.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      state.addWhere(expr(node.getCondition()));
      return null;
    }

    @Override
    public Void visitEval(Eval node, Void ignored) {
      walkChild(node);
      // Eval extends the row set; if a row-cap already pinned it, wrap.
      if (state.orderBy != null || state.fetch != null) {
        state.wrap();
      }
      for (Let let : node.getExpressionList()) {
        state.addEvalAlias(expr(let.getExpression()), letName(let));
      }
      return null;
    }

    @Override
    public Void visitProject(Project node, Void ignored) {
      walkChild(node);
      if (node.isExcluded()) {
        throw new UnsupportedOperationException("fields - is not supported in SqlNode POC yet");
      }
      // PPL wraps every parsed query in a synthesized top-level "project AllFields" — i.e. the
      // bare PPL "source=T" implicitly ends with "| fields *". On OpenSearch indices this
      // expansion picks up metadata fields (_id, _index, _score, ...) which PPL hides; if any
      // are present we must enumerate the non-meta columns. Otherwise leave it as a no-op so
      // downstream pipes (especially Sort) aren't wrapped into informational subqueries.
      if (isSelectStar(node)) {
        // Only enumerate-and-filter metadata when the projection is still the default SELECT *
        // (no upstream pipe customised it). If a Stats / Eval / Project / Window already set
        // a projection, they own the row shape — don't overwrite.
        if (rowTypeOracle != null
            && state.from != null
            && state.projection == null
            && !state.evalExtended) {
          List<String> cols = deriveColumnNames(state.from);
          boolean hasMeta =
              cols.stream()
                  .anyMatch(
                      org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          ::containsKey);
          if (hasMeta) {
            if (state.orderBy != null || state.fetch != null) {
              state.wrap();
            }
            List<SqlNode> projection = new ArrayList<>();
            for (String c : cols) {
              if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                  .containsKey(c)) {
                continue;
              }
              projection.add(new SqlIdentifier(c, POS));
            }
            state.setProjection(projection);
            return null;
          }
        }
        return null;
      }
      // SQL aliases in the SELECT list aren't visible inside the same SELECT list, so a project
      // after an eval/rename/etc. that introduced new names must wrap. Likewise wrap if a
      // row-cap was already applied.
      if (state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null) {
        state.wrap();
      }
      List<SqlNode> selects = new ArrayList<>(node.getProjectList().size());
      for (UnresolvedExpression e : node.getProjectList()) {
        if (e instanceof AllFields) {
          selects.add(SqlIdentifier.star(POS));
        } else if (e instanceof Field f) {
          selects.add(expr(f.getField()));
        } else {
          selects.add(expr(e));
        }
      }
      state.setProjection(selects);
      return null;
    }

    private boolean isSelectStar(Project node) {
      List<UnresolvedExpression> list = node.getProjectList();
      return list.size() == 1 && list.get(0) instanceof AllFields;
    }

    @Override
    public Void visitRename(Rename node, Void ignored) {
      walkChild(node);
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Build origin -> target name map (PPL Rename uses Map nodes with origin=Field,
      // target=Field).
      java.util.Map<String, String> renames = new java.util.HashMap<>();
      for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
        String origin = ((Field) m.getOrigin()).getField().toString();
        String target = ((Field) m.getTarget()).getField().toString();
        renames.put(origin, target);
      }
      List<String> cols = deriveColumnNames(state.from);
      List<SqlNode> selects = new ArrayList<>();
      for (String c : cols) {
        if (renames.containsKey(c)) {
          selects.add(asAlias(new SqlIdentifier(c, POS), renames.get(c)));
        } else {
          selects.add(new SqlIdentifier(c, POS));
        }
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitFlatten(Flatten node, Void ignored) {
      walkChild(node);
      // Need schema to enumerate the struct's sub-fields (named "<flatField>.<sub>" in the
      // existing TableWithStruct convention). Materialize the in-flight FROM and probe its row
      // type via the validator-backed oracle.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      String fieldName = node.getField().getField().toString();
      List<String> allCols = deriveColumnNames(state.from);
      List<String> subCols = allCols.stream().filter(c -> c.startsWith(fieldName + ".")).toList();
      List<String> aliases =
          node.getAliases() != null
              ? node.getAliases()
              : subCols.stream().map(c -> c.substring(fieldName.length() + 1)).toList();
      if (node.getAliases() != null && node.getAliases().size() != subCols.size()) {
        throw new IllegalArgumentException(
            String.format(
                "alias count (%d) doesn't match flattened field count (%d)",
                node.getAliases().size(), subCols.size()));
      }
      // Project all input columns + aliased sub-fields. Building the SELECT list requires us
      // to enumerate inputs (we can't use SELECT * + extras because the sub-field aliases would
      // collide with the existing dotted-name columns).
      List<SqlNode> selects = new ArrayList<>();
      for (String c : allCols) {
        selects.add(new SqlIdentifier(c, POS));
      }
      for (int i = 0; i < subCols.size(); i++) {
        selects.add(asAlias(new SqlIdentifier(subCols.get(i), POS), aliases.get(i)));
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitLookup(Lookup node, Void ignored) {
      walkChild(node);
      state.wrap();
      // Lookup-side: SELECT <output cols>, <key cols> FROM <lookup table>
      // Build by recursively visiting the lookup relation, then projecting just the keys + outputs.
      SqlNode lookupSide = new PplToSqlNode().visit(node.getLookupRelation());
      java.util.Map<String, String> mapping = node.getMappingAliasMap();
      java.util.Map<String, String> output = node.getOutputAliasMap();
      // Project the lookup-side columns we need: outputs first, then mapping keys.
      SqlNodeList lookupSelectList = new SqlNodeList(POS);
      for (java.util.Map.Entry<String, String> e : output.entrySet()) {
        lookupSelectList.add(new SqlIdentifier(e.getKey(), POS));
      }
      for (String key : mapping.keySet()) {
        lookupSelectList.add(new SqlIdentifier(key, POS));
      }
      SqlSelect lookupProject =
          new SqlSelect(
              POS, /* keywordList */
              null,
              lookupSelectList,
              lookupSide, /* where */
              null,
              /* group */ null, /* having */
              null, /* windowList */
              null,
              /* qualify */ null, /* orderBy */
              null, /* offset */
              null, /* fetch */
              null,
              /* hints */ null);
      String inputAlias = "lookup_input";
      String lookupAlias = "lookup_t";
      SqlNode aliasedLookup =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(lookupProject, new SqlIdentifier(lookupAlias, POS)),
              POS);
      SqlNode aliasedInput =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(state.from, new SqlIdentifier(inputAlias, POS)), POS);
      // Build join condition: input.<key> = lookupAlias.<key> for each mapping entry.
      SqlNode condition = null;
      for (java.util.Map.Entry<String, String> e : mapping.entrySet()) {
        SqlNode left = new SqlIdentifier(java.util.Arrays.asList(inputAlias, e.getKey()), POS);
        SqlNode right = new SqlIdentifier(java.util.Arrays.asList(lookupAlias, e.getValue()), POS);
        SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(left, right), POS);
        condition =
            condition == null
                ? eq
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(condition, eq), POS);
      }
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInput,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.LEFT.symbol(POS),
              aliasedLookup,
              org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
              condition);
      state.from = join;
      if (node.getOutputStrategy() == Lookup.OutputStrategy.REPLACE) {
        // REPLACE: drop input columns that collide with the renamed lookup outputs, then append
        // the lookup outputs under their target names. Requires schema enumeration of the input.
        java.util.Set<String> overwritten = new java.util.HashSet<>(output.values());
        List<String> inputCols = deriveColumnNames(aliasedInput);
        // The aliased input's schema is the input's row type; the join just appends lookup cols.
        List<SqlNode> selects = new ArrayList<>();
        for (String c : inputCols) {
          if (overwritten.contains(c)) {
            continue;
          }
          selects.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
        }
        for (java.util.Map.Entry<String, String> e : output.entrySet()) {
          SqlNode lookupCol =
              new SqlIdentifier(java.util.Arrays.asList(lookupAlias, e.getKey()), POS);
          if (e.getKey().equals(e.getValue())) {
            selects.add(lookupCol);
          } else {
            selects.add(asAlias(lookupCol, e.getValue()));
          }
        }
        state.setProjection(selects);
      }
      return null;
    }

    @Override
    public Void visitExpand(Expand node, Void ignored) {
      walkChild(node);
      // Always wrap; expand changes the row set (each input row becomes N rows).
      state.wrap();
      String fieldName;
      UnresolvedExpression fieldExpr = node.getField().getField();
      if (fieldExpr instanceof QualifiedName qn) {
        fieldName = qn.toString();
      } else {
        throw new UnsupportedOperationException(
            "expand requires a simple column reference, got: " + fieldExpr.getClass());
      }
      String alias = node.getAlias() != null ? node.getAlias() : fieldName;
      // Build SQL: SELECT <input>.*, t.<alias> FROM (<input>) AS s, UNNEST(s.<field>) AS t(<alias>)
      // We achieve the implicit-LATERAL CROSS JOIN by setting state.from to a SqlJoin with COMMA.
      String inputAlias = "expand_input";
      SqlNode aliasedInput =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(state.from, new SqlIdentifier(inputAlias, POS)), POS);
      SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, fieldName), POS);
      SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
      SqlNode aliasedUnnest =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(unnest, new SqlIdentifier("expand_t", POS), new SqlIdentifier(alias, POS)),
              POS);
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInput,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.COMMA.symbol(POS),
              aliasedUnnest,
              org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
              null);
      state.from = join;
      // Reset projection to allow downstream pipes to see all columns including the unnested one.
      state.projection = null;
      state.projectionReplaced = false;
      state.evalExtended = false;
      return null;
    }

    @Override
    public Void visitParse(Parse node, Void ignored) {
      walkChild(node);
      org.opensearch.sql.ast.expression.ParseMethod parseMethod = node.getParseMethod();
      if (parseMethod == org.opensearch.sql.ast.expression.ParseMethod.PATTERNS) {
        throw new UnsupportedOperationException(
            "patterns method via Parse not supported in SqlNode POC yet");
      }
      String patternValue = (String) node.getPattern().getValue();
      List<String> groupCandidates =
          org.opensearch.sql.utils.ParseUtils.getNamedGroupCandidates(
              parseMethod, patternValue, node.getArguments());
      if (groupCandidates.isEmpty()) {
        return null;
      }
      if (state.orderBy != null || state.fetch != null) {
        state.wrap();
      }
      SqlNode source = expr(node.getSourceField());
      SqlNode patternLit = SqlLiteral.createCharString(patternValue, POS);
      // Mirror the existing path: PARSE(field, pattern, 'regex'|'grok') for REGEX/GROK; the
      // resulting MAP-typed value is then indexed by the named group via ITEM (i.e. arr[name]).
      SqlNode methodLit = SqlLiteral.createCharString(parseMethod.getName(), POS);
      for (String group : groupCandidates) {
        SqlNode inner =
            new SqlBasicCall(
                new org.apache.calcite.sql.SqlUnresolvedFunction(
                    new SqlIdentifier("PARSE", POS),
                    null,
                    null,
                    null,
                    null,
                    org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                List.of(source, patternLit, methodLit),
                POS);
        SqlNode itemCall =
            new SqlBasicCall(
                SqlStdOperatorTable.ITEM,
                List.of(inner, SqlLiteral.createCharString(group, POS)),
                POS);
        state.addEvalAlias(itemCall, group);
      }
      return null;
    }

    @Override
    public Void visitDedupe(Dedupe node, Void ignored) {
      walkChild(node);
      List<Argument> opts = node.getOptions();
      int allowedDup = (Integer) opts.get(0).getValue().getValue();
      boolean keepEmpty = (Boolean) opts.get(1).getValue().getValue();
      boolean consecutive = (Boolean) opts.get(2).getValue().getValue();
      if (allowedDup <= 0) {
        throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
      }
      if (consecutive) {
        throw new UnsupportedOperationException(
            "Consecutive deduplication is not supported in the SqlNode POC");
      }
      // Step 1: if !keepEmpty, add IS NOT NULL filters on the dedup fields.
      List<SqlNode> fieldNodes = new ArrayList<>(node.getFields().size());
      for (Field f : node.getFields()) {
        fieldNodes.add(expr(f.getField()));
      }
      if (!keepEmpty) {
        // Wrap if any prior pipe state would conflict with adding pure filters.
        if (state.evalExtended || state.projectionReplaced) {
          state.wrap();
        }
        for (SqlNode field : fieldNodes) {
          state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(field), POS));
        }
      }
      // Step 2: extend projection with `ROW_NUMBER() OVER (PARTITION BY ...) AS
      // _row_number_dedup_`.
      SqlNodeList partitionBy = new SqlNodeList(POS);
      for (SqlNode field : fieldNodes) {
        partitionBy.add(field);
      }
      SqlNode rowNumberWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              partitionBy,
              new SqlNodeList(POS),
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode rowNumberOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                  rowNumberWindow),
              POS);
      state.addEvalAlias(rowNumberOver, "_row_number_dedup_");
      // Step 3: wrap and filter on _row_number_dedup_ <= allowedDup.
      state.wrap();
      SqlNode rowCol = new SqlIdentifier("_row_number_dedup_", POS);
      SqlNode boundCheck =
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL, List.of(rowCol, intLiteral(allowedDup)), POS);
      if (keepEmpty) {
        // (field IS NULL) OR ... OR (_row_number_dedup_ <= N)
        SqlNode predicate = boundCheck;
        for (SqlNode field : fieldNodes) {
          predicate =
              new SqlBasicCall(
                  SqlStdOperatorTable.OR,
                  List.of(
                      new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(field), POS),
                      predicate),
                  POS);
        }
        state.addWhere(predicate);
      } else {
        state.addWhere(boundCheck);
      }
      // Step 4: drop the helper column on the way out.
      state.wrap();
      // No explicit projection needed — caller's downstream pipe (or an automatic *) shows the
      // _row_number_dedup_ column too. The existing path adds an explicit Project that strips
      // the helper. Mirror that by setting projection = list of original column refs is not
      // possible without schema info; instead we leave it and rely on a downstream `fields`
      // pipe in PPL to drop it. Tests that check for the Project layer will need to assert on
      // the inner shape.
      return null;
    }

    @Override
    public Void visitStreamWindow(StreamWindow node, Void ignored) {
      walkChild(node);
      if (node.getResetBefore() != null || node.getResetAfter() != null) {
        throw new UnsupportedOperationException(
            "streamstats reset_before/reset_after not yet supported in SqlNode POC");
      }
      if (!node.getGroupList().isEmpty()) {
        throw new UnsupportedOperationException(
            "streamstats with `by` requires a synthetic __stream_seq__ helper column not yet"
                + " implemented in the SqlNode POC");
      }
      if (node.getWindow() > 0) {
        throw new UnsupportedOperationException(
            "streamstats `window=N` not yet supported in SqlNode POC");
      }
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // ROWS frame: UNBOUNDED PRECEDING → (CURRENT ROW or 1 PRECEDING based on `current`).
      SqlNode lower = org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS);
      SqlNode upper =
          node.isCurrent()
              ? org.apache.calcite.sql.SqlWindow.createCurrentRow(POS)
              : new SqlBasicCall(
                  org.apache.calcite.sql.SqlWindow.PRECEDING_OPERATOR, List.of(intLiteral(1)), POS);
      List<SqlNode> selects = new ArrayList<>();
      selects.add(SqlIdentifier.star(POS));
      for (UnresolvedExpression item : node.getWindowFunctionList()) {
        Alias al = (Alias) item;
        WindowFunction wf = (WindowFunction) al.getDelegated();
        SqlNode aggNode = aggCall(wf.getFunction());
        SqlNode window =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(POS),
                /* isRows */ SqlLiteral.createBoolean(true, POS),
                lower,
                upper,
                null,
                POS);
        SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
        selects.add(asAlias(over, al.getName()));
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitWindow(Window node, Void ignored) {
      walkChild(node);
      // eventstats appends agg-OVER columns to the row. Wrap if there's pending state that would
      // make alias visibility ambiguous in the new projection.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Projection is "*, <each window func> AS <alias>".
      List<SqlNode> selects = new ArrayList<>();
      selects.add(SqlIdentifier.star(POS));
      SqlNodeList partitionBy = new SqlNodeList(POS);
      for (UnresolvedExpression p : node.getGroupList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        partitionBy.add(expr(core));
      }
      for (UnresolvedExpression item : node.getWindowFunctionList()) {
        Alias al = (Alias) item;
        WindowFunction wf = (WindowFunction) al.getDelegated();
        SqlNode aggNode = aggCall(wf.getFunction());
        SqlNode window =
            org.apache.calcite.sql.SqlWindow.create(
                /* declName */ null,
                /* refName */ null,
                partitionBy,
                /* orderList */ new SqlNodeList(POS),
                /* isRows */ SqlLiteral.createBoolean(false, POS),
                /* lowerBound */ null,
                /* upperBound */ null,
                /* allowPartial */ null,
                POS);
        SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
        selects.add(asAlias(over, al.getName()));
      }
      state.setProjection(selects);
      // Mark projection as replaced so subsequent pipes wrap, but we must NOT mark evalExtended
      // (that would force the next where to wrap unnecessarily; aliases here are visible to
      // downstream pipes via the wrap).
      return null;
    }

    @Override
    public Void visitAggregation(Aggregation node, Void ignored) {
      walkChild(node);
      // Aggregation always changes the row set; wrap any pending pipe state into a subquery so
      // GROUP BY operates on the input rows, not the post-aggregation rows.
      if (state.where != null
          || state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null) {
        state.wrap();
      }
      List<SqlNode> selects = new ArrayList<>();
      List<SqlNode> groupKeys = new ArrayList<>();
      if (node.getGroupExprList() != null) {
        for (UnresolvedExpression g : node.getGroupExprList()) {
          SqlNode key;
          String alias = null;
          if (g instanceof Alias a) {
            key = expr(a.getDelegated());
            alias = a.getName();
          } else {
            key = expr(g);
          }
          groupKeys.add(key);
          // Group keys are part of the select list so downstream pipes can reference them.
          selects.add(alias != null ? asAlias(key, alias) : key);
        }
      }
      for (UnresolvedExpression a : node.getAggExprList()) {
        if (a instanceof Alias al) {
          selects.add(asAlias(aggCall(al.getDelegated()), al.getName()));
        } else {
          selects.add(aggCall(a));
        }
      }
      state.setProjection(selects);
      state.setGroupBy(groupKeys);
      return null;
    }

    @Override
    public Void visitSort(Sort node, Void ignored) {
      walkChild(node);
      List<SqlNode> keys = new ArrayList<>(node.getSortList().size());
      for (Field f : node.getSortList()) {
        SqlNode key = expr(f.getField());
        Sort.SortOption opt = analyzeSortOption(f.getFieldArgs());
        if (opt.getSortOrder() == Sort.SortOrder.DESC) {
          key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
        }
        SqlOperator nullsOp =
            opt.getNullOrder() == Sort.NullOrder.NULL_LAST
                ? SqlStdOperatorTable.NULLS_LAST
                : SqlStdOperatorTable.NULLS_FIRST;
        key = new SqlBasicCall(nullsOp, List.of(key), POS);
        keys.add(key);
      }
      state.setOuterOrderBy(keys);
      if (node.getCount() != null && node.getCount() != 0) {
        state.setOuterFetch(intLiteral(node.getCount()));
      }
      return null;
    }

    @Override
    public Void visitHead(Head node, Void ignored) {
      walkChild(node);
      state.setOuterFetch(intLiteral(node.getSize()));
      return null;
    }

    @Override
    public Void visitLimit(Limit node, Void ignored) {
      walkChild(node);
      state.setOuterFetch(intLiteral(node.getLimit()));
      return null;
    }
  }

  /** Mutable in-flight SqlSelect being assembled. */
  private static final class Pipeline {
    SqlNode from;
    SqlNode where;

    /** {@code null} means "SELECT *" (un-modified projection). */
    List<SqlNode> projection;

    List<SqlNode> groupBy;
    List<SqlNode> orderBy;
    SqlNode fetch;

    /**
     * PPL preserves SORT/HEAD effects through downstream pipes. SQL ORDER BY in a subquery is
     * informational only — so we accumulate them at the pipeline level and apply as the outermost
     * {@link SqlOrderBy} once the entire pipeline has been walked.
     */
    List<SqlNode> outerOrderBy;

    SqlNode outerFetch;

    /** True when an Eval pipe added alias columns to the projection. */
    boolean evalExtended;

    /** True when a Project pipe replaced the projection list. */
    boolean projectionReplaced;

    void setFrom(SqlNode f) {
      from = f;
    }

    void addWhere(SqlNode cond) {
      where =
          where == null
              ? cond
              : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(where, cond), POS);
    }

    void addEvalAlias(SqlNode expr, String alias) {
      if (projection == null) {
        projection = new ArrayList<>();
        projection.add(SqlIdentifier.star(POS));
      }
      projection.add(asAlias(expr, alias));
      evalExtended = true;
    }

    void setProjection(List<SqlNode> list) {
      projection = list;
      projectionReplaced = true;
    }

    void setGroupBy(List<SqlNode> keys) {
      groupBy = keys;
    }

    void setOrderBy(List<SqlNode> keys) {
      orderBy = keys;
    }

    void setFetch(SqlNode f) {
      fetch = f;
    }

    void setOuterOrderBy(List<SqlNode> keys) {
      outerOrderBy = keys;
    }

    void setOuterFetch(SqlNode f) {
      outerFetch = f;
    }

    /** Close the current SqlSelect and start a new one whose FROM is the just-closed select. */
    void wrap() {
      from = toSqlNode();
      where = null;
      projection = null;
      groupBy = null;
      orderBy = null;
      fetch = null;
      evalExtended = false;
      projectionReplaced = false;
      // outerOrderBy/outerFetch are deliberately preserved across wraps — they apply at the
      // outermost level of the final SqlNode tree, regardless of pipe nesting.
    }

    /**
     * Build the final SqlNode tree for the whole pipeline: take the in-flight select and wrap it in
     * a top-level {@link SqlOrderBy} carrying any pending outer sort/fetch from upstream
     * SORT/HEAD/LIMIT pipes that need to survive subsequent pipes.
     */
    SqlNode toFinalSqlNode() {
      // The validator needs a query expression (SELECT ...) at the top level, never a bare
      // table identifier. Force SELECT * FROM <table> when nothing else is set so a bare
      // `source=test` query reaches validation as a complete SELECT.
      SqlNode body;
      if (where == null
          && projection == null
          && groupBy == null
          && orderBy == null
          && fetch == null
          && from instanceof SqlIdentifier) {
        SqlNodeList selectList = new SqlNodeList(POS);
        selectList.add(SqlIdentifier.star(POS));
        body =
            new SqlSelect(
                POS, /* keywordList */
                null,
                selectList,
                from, /* where */
                null,
                /* group */ null, /* having */
                null, /* windowList */
                null,
                /* qualify */ null, /* orderBy */
                null, /* offset */
                null, /* fetch */
                null,
                /* hints */ null);
      } else {
        body = toSqlNode();
      }
      if (outerOrderBy != null || outerFetch != null) {
        SqlNodeList ord = new SqlNodeList(POS);
        if (outerOrderBy != null) {
          for (SqlNode n : outerOrderBy) {
            ord.add(n);
          }
        }
        return new SqlOrderBy(POS, body, ord, /* offset */ null, outerFetch);
      }
      return body;
    }

    SqlNode toSqlNode() {
      // Nothing populated besides a bare table reference — return identifier directly.
      if (where == null
          && projection == null
          && groupBy == null
          && orderBy == null
          && fetch == null
          && from instanceof SqlIdentifier) {
        return from;
      }
      SqlNodeList selectList = new SqlNodeList(POS);
      if (projection == null) {
        selectList.add(SqlIdentifier.star(POS));
      } else {
        for (SqlNode n : projection) {
          selectList.add(n);
        }
      }
      SqlNodeList groupList = null;
      if (groupBy != null && !groupBy.isEmpty()) {
        groupList = new SqlNodeList(POS);
        for (SqlNode n : groupBy) {
          groupList.add(n);
        }
      }
      // Build a plain SELECT ... FROM ... [WHERE ...] [GROUP BY ...]; ORDER BY / FETCH go into a
      // wrapping SqlOrderBy. Putting them on the SqlSelect directly trips Calcite's
      // precedence-driven subquery-wrap path during unparse and during validation, dropping the
      // order on the outermost select.
      SqlSelect select =
          new SqlSelect(
              POS,
              /* keywordList */ null,
              selectList,
              from,
              where,
              groupList,
              /* having */ null,
              /* windowList */ null,
              /* qualify */ null,
              /* orderBy */ null,
              /* offset */ null,
              /* fetch */ null,
              /* hints */ null);
      if (orderBy != null || fetch != null) {
        SqlNodeList ord = new SqlNodeList(POS);
        if (orderBy != null) {
          for (SqlNode n : orderBy) {
            ord.add(n);
          }
        }
        return new SqlOrderBy(POS, select, ord, /* offset */ null, fetch);
      }
      return select;
    }
  }

  /** Mirror of {@code CalciteRelNodeVisitor.analyzeSortOption} kept lightweight here. */
  private static Sort.SortOption analyzeSortOption(List<Argument> args) {
    boolean desc = false;
    for (Argument a : args) {
      if ("asc".equalsIgnoreCase(a.getArgName())) {
        desc = !((Boolean) a.getValue().getValue());
      }
    }
    return desc ? Sort.SortOption.DEFAULT_DESC : Sort.SortOption.DEFAULT_ASC;
  }

  // -- Expression translation -------------------------------------------------

  private SqlNode expr(UnresolvedExpression e) {
    if (e instanceof Literal lit) return literal(lit);
    if (e instanceof QualifiedName qn) return qualifiedNameToFieldIdentifier(qn);
    if (e instanceof Field f) return expr(f.getField());
    if (e instanceof Compare c) return cmp(c);
    if (e instanceof And a)
      return new SqlBasicCall(
          SqlStdOperatorTable.AND, List.of(expr(a.getLeft()), expr(a.getRight())), POS);
    if (e instanceof Or o)
      return new SqlBasicCall(
          SqlStdOperatorTable.OR, List.of(expr(o.getLeft()), expr(o.getRight())), POS);
    if (e instanceof Function f) return func(f);
    if (e instanceof Case c) return caseExpr(c);
    if (e instanceof Not n)
      return new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(expr(n.getExpression())), POS);
    if (e instanceof In in) return inExpr(in);
    if (e instanceof InSubquery is) return inSubqueryExpr(is);
    throw new UnsupportedOperationException(
        "Expression not yet supported in SqlNode POC: " + e.getClass().getSimpleName());
  }

  private SqlNode literal(Literal lit) {
    Object v = lit.getValue();
    DataType t = lit.getType();
    if (v == null || t == DataType.NULL) {
      return SqlLiteral.createNull(POS);
    }
    return switch (t) {
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) v, POS);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(v.toString(), POS);
      case FLOAT, DOUBLE, DECIMAL -> {
        BigDecimal bd = (v instanceof BigDecimal b) ? b : new BigDecimal(v.toString());
        yield SqlLiteral.createExactNumeric(bd.toPlainString(), POS);
      }
      case STRING -> SqlLiteral.createCharString(v.toString(), POS);
      default -> throw new UnsupportedOperationException("Literal type not yet supported: " + t);
    };
  }

  private SqlNode cmp(Compare c) {
    SqlOperator op =
        switch (c.getOperator()) {
          case "=" -> SqlStdOperatorTable.EQUALS;
          case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
          case ">" -> SqlStdOperatorTable.GREATER_THAN;
          case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
          case "<" -> SqlStdOperatorTable.LESS_THAN;
          case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
          default ->
              throw new UnsupportedOperationException(
                  "Compare operator not supported: " + c.getOperator());
        };
    return new SqlBasicCall(op, List.of(expr(c.getLeft()), expr(c.getRight())), POS);
  }

  private SqlNode aggCall(UnresolvedExpression e) {
    // PPL eventstats wraps aggregates as WindowFunction(Function("count", [])); stats wraps them
    // as AggregateFunction. Normalize to a (name, arg-or-null) shape so the same operator-name
    // dispatch handles both.
    AggregateFunction af;
    if (e instanceof AggregateFunction a) {
      af = a;
    } else if (e instanceof Function f) {
      UnresolvedExpression arg = f.getFuncArgs().isEmpty() ? null : f.getFuncArgs().get(0);
      af = new AggregateFunction(f.getFuncName(), arg);
    } else {
      throw new UnsupportedOperationException(
          "stats aggregator must be a Function or AggregateFunction, got: "
              + e.getClass().getSimpleName());
    }
    SqlOperator op =
        switch (af.getFuncName().toLowerCase()) {
          case "count" -> SqlStdOperatorTable.COUNT;
          case "sum" -> SqlStdOperatorTable.SUM;
          case "avg" -> SqlStdOperatorTable.AVG;
          case "min" -> SqlStdOperatorTable.MIN;
          case "max" -> SqlStdOperatorTable.MAX;
          default ->
              throw new UnsupportedOperationException(
                  "Aggregate not yet wired in SqlNode POC: " + af.getFuncName());
        };
    // PPL `count()` parses as count(AllFields). Map to SQL `COUNT(*)`.
    SqlNode arg;
    if (af.getField() == null || af.getField() instanceof AllFields) {
      arg = SqlIdentifier.star(POS);
    } else {
      arg = expr(af.getField());
    }
    SqlLiteral quantifier =
        af.getDistinct()
            ? SqlLiteral.createSymbol(org.apache.calcite.sql.SqlSelectKeyword.DISTINCT, POS)
            : null;
    return new SqlBasicCall(op, List.of(arg), POS, quantifier);
  }

  private SqlNode inSubqueryExpr(InSubquery is) {
    // Compile the subquery plan with a fresh visitor — it produces its own SqlSelect tree.
    SqlNode subQuery = new PplToSqlNode().visit(is.getQuery());
    SqlNode left;
    if (is.getValue().size() == 1) {
      left = expr(is.getValue().get(0));
    } else {
      // Multi-column IN — wrap as a row.
      List<SqlNode> rowOperands = new ArrayList<>(is.getValue().size());
      for (UnresolvedExpression v : is.getValue()) {
        rowOperands.add(expr(v));
      }
      left = new SqlBasicCall(SqlStdOperatorTable.ROW, rowOperands, POS);
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(left, subQuery), POS);
  }

  private SqlNode inExpr(In in) {
    SqlNodeList values = new SqlNodeList(POS);
    for (UnresolvedExpression v : in.getValueList()) {
      values.add(expr(v));
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(expr(in.getField()), values), POS);
  }

  private SqlNode caseExpr(Case c) {
    SqlNodeList whens = new SqlNodeList(POS);
    SqlNodeList thens = new SqlNodeList(POS);
    for (org.opensearch.sql.ast.expression.When w : c.getWhenClauses()) {
      whens.add(expr(w.getCondition()));
      thens.add(expr(w.getResult()));
    }
    SqlNode elseNode = c.getElseClause().map(this::expr).orElse(SqlLiteral.createNull(POS));
    SqlNode caseValue = c.getCaseValue() == null ? null : expr(c.getCaseValue());
    return new org.apache.calcite.sql.fun.SqlCase(POS, caseValue, whens, thens, elseNode);
  }

  private SqlNode func(Function f) {
    List<SqlNode> args = new ArrayList<>(f.getFuncArgs().size());
    for (UnresolvedExpression a : f.getFuncArgs()) {
      args.add(expr(a));
    }
    SqlOperator op = arithmeticOperator(f.getFuncName());
    if (op != null) {
      return new SqlBasicCall(op, args, POS);
    }
    // Defer function resolution to the validator: build an unresolved function call that
    // SqlValidator will resolve against PPLBuiltinOperators + SqlStdOperatorTable, performing
    // overload selection and type coercion as standard SQL would.
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier(f.getFuncName(), POS),
            /* returnTypeInference */ null,
            /* operandTypeInference */ null,
            /* operandTypeChecker */ null,
            /* paramTypes */ null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        args,
        POS);
  }

  /**
   * PPL parses arithmetic operators as Function nodes with names like "+"/"-"/etc. Map those to
   * Calcite operators directly; everything else goes through the validator's name lookup.
   */
  private SqlOperator arithmeticOperator(String name) {
    return switch (name) {
      case "+" -> SqlStdOperatorTable.PLUS;
      case "-" -> SqlStdOperatorTable.MINUS;
      case "*" -> SqlStdOperatorTable.MULTIPLY;
      case "/" -> SqlStdOperatorTable.DIVIDE;
      case "%" -> SqlStdOperatorTable.MOD;
      default -> null;
    };
  }

  // -- Helpers ---------------------------------------------------------------

  private static SqlNode intLiteral(int v) {
    return SqlLiteral.createExactNumeric(Integer.toString(v), POS);
  }

  private static SqlNodeList starList() {
    SqlNodeList l = new SqlNodeList(POS);
    l.add(SqlIdentifier.star(POS));
    return l;
  }

  private static SqlNode asAlias(SqlNode expr, String alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(expr, new SqlIdentifier(alias, POS)), POS);
  }

  /**
   * Build a multi-part identifier suitable for a table reference (FROM clause). For a name like
   * `schema.table`, the validator interprets each part as a catalog level.
   */
  private static SqlIdentifier qualifiedNameToIdentifier(QualifiedName qn) {
    return new SqlIdentifier(qn.getParts(), POS);
  }

  /**
   * Build a single-part identifier suitable for a column reference. PPL parses `obj.sub` as a
   * QualifiedName with two parts, but on OpenSearch indices the column name is the literal dotted
   * string ("obj.sub"), not a navigation into a struct. Joining the parts back into one identifier
   * component matches that storage shape.
   */
  private static SqlIdentifier qualifiedNameToFieldIdentifier(QualifiedName qn) {
    return new SqlIdentifier(qn.toString(), POS);
  }

  private static String letName(Let let) {
    UnresolvedExpression inner = let.getVar().getField();
    if (inner instanceof QualifiedName qn) {
      return qn.toString();
    }
    return inner.toString();
  }
}
