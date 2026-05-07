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
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

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

  /** Public entry point. */
  public SqlNode visit(UnresolvedPlan plan) {
    Pipeline state = new Pipeline();
    new Builder(state).walk(plan);
    return state.toSqlNode();
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
      state.setFrom(qualifiedNameToIdentifier(node.getTableQualifiedName()));
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
      // bare PPL "source=T" implicitly ends with "| fields *". We must not let that no-op
      // wrap a downstream pipe (especially Sort) since wrapping pushes ORDER BY into a
      // subquery where SQL semantics treat it as informational.
      if (isSelectStar(node)) {
        return null;
      }
      // SQL aliases in the SELECT list aren't visible inside the same SELECT list, so a project
      // after an eval must wrap. Likewise wrap if a row-cap was already applied.
      if (state.evalExtended || state.orderBy != null || state.fetch != null) {
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
    public Void visitSort(Sort node, Void ignored) {
      walkChild(node);
      if (state.fetch != null) {
        state.wrap();
      }
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
      state.setOrderBy(keys);
      if (node.getCount() != null && node.getCount() != 0) {
        state.setFetch(intLiteral(node.getCount()));
      }
      return null;
    }

    @Override
    public Void visitHead(Head node, Void ignored) {
      walkChild(node);
      if (state.fetch != null) {
        state.wrap();
      }
      state.setFetch(intLiteral(node.getSize()));
      return null;
    }

    @Override
    public Void visitLimit(Limit node, Void ignored) {
      walkChild(node);
      if (state.fetch != null) {
        state.wrap();
      }
      state.setFetch(intLiteral(node.getLimit()));
      return null;
    }
  }

  /** Mutable in-flight SqlSelect being assembled. */
  private static final class Pipeline {
    SqlNode from;
    SqlNode where;

    /** {@code null} means "SELECT *" (un-modified projection). */
    List<SqlNode> projection;

    List<SqlNode> orderBy;
    SqlNode fetch;

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

    void setOrderBy(List<SqlNode> keys) {
      orderBy = keys;
    }

    void setFetch(SqlNode f) {
      fetch = f;
    }

    /** Close the current SqlSelect and start a new one whose FROM is the just-closed select. */
    void wrap() {
      from = toSqlNode();
      where = null;
      projection = null;
      orderBy = null;
      fetch = null;
      evalExtended = false;
      projectionReplaced = false;
    }

    SqlNode toSqlNode() {
      // Nothing populated besides a bare table reference — return identifier directly.
      if (where == null
          && projection == null
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
      // Build a plain SELECT ... FROM ... [WHERE ...]; ORDER BY / FETCH go into a wrapping
      // SqlOrderBy. Putting them on the SqlSelect directly trips Calcite's precedence-driven
      // subquery-wrap path during unparse and during validation, dropping the order on the
      // outermost select.
      SqlSelect select =
          new SqlSelect(
              POS, /* keywordList */
              null,
              selectList,
              from,
              where,
              /* group */ null, /* having */
              null, /* windowList */
              null,
              /* qualify */ null, /* orderBy */
              null, /* offset */
              null, /* fetch */
              null,
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
    if (e instanceof QualifiedName qn) return qualifiedNameToIdentifier(qn);
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
    SqlOperator op = lookupOperator(f.getFuncName());
    List<SqlNode> args = new ArrayList<>(f.getFuncArgs().size());
    for (UnresolvedExpression a : f.getFuncArgs()) {
      args.add(expr(a));
    }
    return new SqlBasicCall(op, args, POS);
  }

  /** Tiny first-cut name→operator map. Will eventually be replaced by validator overload lookup. */
  private SqlOperator lookupOperator(String name) {
    return switch (name.toLowerCase()) {
      case "+" -> SqlStdOperatorTable.PLUS;
      case "-" -> SqlStdOperatorTable.MINUS;
      case "*" -> SqlStdOperatorTable.MULTIPLY;
      case "/" -> SqlStdOperatorTable.DIVIDE;
      case "abs" -> SqlStdOperatorTable.ABS;
      case "upper" -> SqlStdOperatorTable.UPPER;
      case "lower" -> SqlStdOperatorTable.LOWER;
      default ->
          throw new UnsupportedOperationException("Function not yet wired in SqlNode POC: " + name);
    };
  }

  // -- Helpers ---------------------------------------------------------------

  private static SqlNode intLiteral(int v) {
    return SqlLiteral.createExactNumeric(Integer.toString(v), POS);
  }

  private static SqlNode asAlias(SqlNode expr, String alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(expr, new SqlIdentifier(alias, POS)), POS);
  }

  private static SqlIdentifier qualifiedNameToIdentifier(QualifiedName qn) {
    return new SqlIdentifier(qn.getParts(), POS);
  }

  private static String letName(Let let) {
    UnresolvedExpression inner = let.getVar().getField();
    if (inner instanceof QualifiedName qn) {
      return qn.toString();
    }
    return inner.toString();
  }
}
