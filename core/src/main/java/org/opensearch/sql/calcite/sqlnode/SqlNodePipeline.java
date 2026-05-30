/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Routes a PPL-produced {@link RelNode} through {@code RelToSqlConverter} → {@link SqlValidator} →
 * {@link SqlToRelConverter}. The validator becomes the single source of type truth, replacing the
 * home-grown coercion plumbing.
 *
 * <p>Both sides of the round-trip share the same {@link OpenSearchTypeFactory} and catalog, so UDT
 * identity is regenerated from the catalog on the validator side rather than transmitted through
 * the SQL string.
 */
public final class SqlNodePipeline {

  /**
   * Operator table for storage-engine-supplied operators (e.g. {@code DISTINCT_COUNT_APPROX},
   * {@code GEOIP}). Storage modules register their {@link SqlOperatorTable} here at boot so the
   * validator can resolve names that originate from those operators after the RelNode → SQL →
   * RelNode round-trip.
   */
  private static final AtomicReference<SqlOperatorTable> EXTENSION_OPERATOR_TABLE =
      new AtomicReference<>();

  private SqlNodePipeline() {}

  /**
   * Registers the operator table contributed by storage extensions (e.g. the OpenSearch execution
   * engine's dynamic {@code OperatorTable}). Called once at boot from the extension side.
   */
  public static void registerExtensionOperatorTable(SqlOperatorTable table) {
    EXTENSION_OPERATOR_TABLE.set(table);
  }

  /**
   * Builds the operator table used by the validator. PPL ships its own variants of common
   * aggregates (e.g. {@code AVG_NULLABLE}, FIRST/LAST UDAFs) registered under the same names as
   * Calcite's stock operators. A naive {@link ChainedSqlOperatorTable} returns all matches; if two
   * candidates share the same name, {@code SqlUtil.lookupRoutine} falls into {@code
   * filterRoutinesByTypePrecedence} which rejects every operator whose {@link
   * org.apache.calcite.sql.type.SqlOperandTypeChecker#isFixedParameters()} is the default {@code
   * false} (true for {@code FamilyOperandTypeChecker}-based checkers like {@code
   * OperandTypes.NUMERIC}). The result is "No match found for function signature AVG(<NUMERIC>)"
   * even though both AVGs are registered. Resolve this by shadowing: if the PPL table returns any
   * match for a name, the stock table is skipped for that name.
   *
   * <p>The optional extension table (registered via {@link #registerExtensionOperatorTable}) is
   * consulted before the standard table. It carries storage-engine operators such as {@code
   * DISTINCT_COUNT_APPROX} that are added at runtime — without this hop, the validator would not
   * see them and fail with "No match found for function signature".
   */
  private static SqlOperatorTable buildOperatorTable() {
    SqlOperatorTable ppl = PPLBuiltinOperators.instance();
    SqlOperatorTable extension = EXTENSION_OPERATOR_TABLE.get();
    SqlOperatorTable std = SqlStdOperatorTable.instance();
    // Library operators provide dialect-specific functions that the validator needs to resolve
    // after RelToSqlConverter unparses the plan:
    //   - BigQuery: SAFE_CAST (visitor emits SAFE_CAST RexCalls via makeCast(safe=true);
    //     RelToSqlConverter unparses as SAFE_CAST(x AS type)).
    //   - Spark: MAP (PPL relevance UDFs like query_string/match carry their named arguments as
    //     SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR RexCalls; the Spark dialect unparses these as
    //     bare MAP(k, v) function calls. Without SqlLibrary.SPARK, the validator can't resolve
    //     "MAP" as a FUNCTION-syntax operator and rejects the round-tripped SQL with "No match
    //     found for function signature MAP(<CHARACTER>, <CHARACTER>)".)
    //   - PostgreSQL: ILIKE / NOT ILIKE (PPL `where x like* '...'` or case-insensitive
    //     comparisons lower to ILIKE; the dialect unparses as ILIKE which only resolves under
    //     the PostgreSQL library).
    //   - MySQL: STRCMP (PPL `strcmp(a, b)` lowers to SqlLibraryOperators.STRCMP which is only
    //     visible under SqlLibrary.MYSQL).
    SqlOperatorTable lib =
        org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY,
            org.apache.calcite.sql.fun.SqlLibrary.SPARK,
            org.apache.calcite.sql.fun.SqlLibrary.HIVE,
            org.apache.calcite.sql.fun.SqlLibrary.MYSQL,
            org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL);
    return new ChainedSqlOperatorTable(
        extension == null
            ? java.util.Arrays.asList(ppl, std, lib)
            : java.util.Arrays.asList(ppl, extension, std, lib)) {
      @Override
      public void lookupOperatorOverloads(
          org.apache.calcite.sql.SqlIdentifier opName,
          org.apache.calcite.sql.SqlFunctionCategory category,
          org.apache.calcite.sql.SqlSyntax syntax,
          java.util.List<org.apache.calcite.sql.SqlOperator> operatorList,
          org.apache.calcite.sql.validate.SqlNameMatcher nameMatcher) {
        int before = operatorList.size();
        ppl.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        // If the caller asked for a non-FUNCTION syntax (e.g. SPECIAL for the EXISTS subquery,
        // PREFIX for unary -, etc.) but the PPL table returned only FUNCTION-syntax operators
        // for that name, the PPL match cannot satisfy the call site. Discard the PPL match and
        // fall through to the stock table so SqlStdOperatorTable.EXISTS / NOT / etc. resolve.
        if (operatorList.size() > before
            && syntax != org.apache.calcite.sql.SqlSyntax.FUNCTION
            && operatorList.subList(before, operatorList.size()).stream()
                .allMatch(op -> op.getSyntax() == org.apache.calcite.sql.SqlSyntax.FUNCTION)) {
          while (operatorList.size() > before) {
            operatorList.remove(operatorList.size() - 1);
          }
        }
        if (operatorList.size() == before && extension != null) {
          extension.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }
        if (operatorList.size() == before) {
          std.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }
        if (operatorList.size() == before) {
          lib.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }
      }
    };
  }

  /**
   * Round-trip {@code original} through SQL and Calcite's validator. Every plan is sent through
   * {@link RelToSqlConverter} → parser → {@link SqlValidator} → {@link SqlToRelConverter}.
   *
   * <p>Aggregate hints (e.g. {@code AGG_ARGS}) are dropped by the round-trip — RelToSql drops them
   * because there is no SQL representation, and SqlValidator builds fresh Aggregate nodes with
   * empty hints. Re-attach them by walking original and round-tripped plans in lock-step and
   * copying hints onto matching Aggregate positions.
   *
   * <p>GraphLookup is the only command that bypasses the round-trip. Its RelNode form ({@code
   * LogicalGraphLookup}) has no SQL representation and {@code RelToSqlConverter} fails with "Error
   * while converting RelNode to SqlNode". Per the project rule, GraphLookup is the single approved
   * bypass; all other commands and functions must go through the validator.
   */
  public static RelNode revalidate(RelNode original, CalcitePlanContext context) {
    if (containsGraphLookup(original)) {
      return original;
    }
    String origStr = org.apache.calcite.plan.RelOptUtil.toString(original);
    boolean trace = origStr.contains("transpose") || origStr.contains("TRANSPOSE");
    if (trace) System.out.println("[TR2-ORIG] " + origStr.replace("\n", "<NL>"));
    String sql = relToSql(original);
    if (trace) System.out.println("[TR2-SQL] " + sql.replace("\n", " | "));
    RelNode roundTripped = sqlToRel(sql, context);
    if (trace) System.out.println("[TR2-RT] " + org.apache.calcite.plan.RelOptUtil.toString(roundTripped).replace("\n", "<NL>"));
    roundTripped = stripIdentityProjects(roundTripped);
    roundTripped = unpadRelevanceMapKeys(roundTripped);
    roundTripped = retypeItemForArrayCast(roundTripped);
    roundTripped = stripHighlightFromExistsTop(roundTripped, context);
    roundTripped = reattachCorrelateJoinTypes(original, roundTripped);
    return reattachAggregateHints(original, roundTripped);
  }

  /**
   * Track K17 (post-pass): when the round-trip's optimizer drops the EXISTS subquery's outer
   * Project (because EXISTS only checks row existence, not columns), the top-of-plan inside the
   * RexSubQuery may expose {@code _highlight} (MAP&lt;VARCHAR, ANY&gt;) directly. Calcite's
   * enumerable codegen then materialises rows including the MAP and trips on
   * "Assignment conversion not possible from java.util.Map to java.lang.Comparable".
   *
   * <p>Wrap the inner plan of every EXISTS {@link org.apache.calcite.rex.RexSubQuery} with a
   * {@link org.apache.calcite.rel.logical.LogicalProject} that drops {@code _highlight}. EXISTS
   * doesn't materialise any column from the inner row into the outer result, so dropping the
   * column is semantically transparent. Skipped when highlight was requested — that flag means
   * the column must remain available for the response formatter.
   */
  private static RelNode stripHighlightFromExistsTop(
      RelNode root, CalcitePlanContext context) {
    if (context.isHighlightRequested()) return root;
    org.apache.calcite.rex.RexShuttle shuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitSubQuery(
              org.apache.calcite.rex.RexSubQuery subQuery) {
            // Recurse into nested subqueries first so inner-most subqueries get their strip too.
            org.apache.calcite.rex.RexSubQuery walked =
                (org.apache.calcite.rex.RexSubQuery) super.visitSubQuery(subQuery);
            RelNode inner = walked.rel;
            // Try a deep scan strip: when the inner subquery has a Linear chain
            // (Project/Filter/Sort) over a TableScan with _highlight at the last position
            // AND no operator in the chain references the _highlight position, drop it at
            // the scan. This prevents Calcite's EnumerableSort/Window codegen from generating
            // `(Comparable) row.fieldX` for the MAP column inside intermediate operators.
            // Applies to all subquery kinds (EXISTS, IN, SCALAR) — the strip is safe
            // whenever no upstream operator references the _highlight position.
            RelNode deeplyStripped = tryDeepStripHighlightInSimpleChain(inner);
            if (deeplyStripped != inner) {
              inner = deeplyStripped;
            }

            // Top-of-plan strip applies ONLY for EXISTS — those don't materialise inner cols
            // into the outer result so dropping the top column is semantically transparent.
            // IN/Scalar subqueries reference specific value columns, so column reordering at
            // the top would break match semantics. The scan-level strip above is safe for all
            // kinds because it only changes the scan's row-type, not the projected output.
            if (walked.getKind() == org.apache.calcite.sql.SqlKind.EXISTS) {
              int hlIdx = -1;
              java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields =
                  inner.getRowType().getFieldList();
              for (int i = 0; i < fields.size(); i++) {
                if ("_highlight".equals(fields.get(i).getName())) {
                  hlIdx = i;
                  break;
                }
              }
              if (hlIdx >= 0) {
                org.apache.calcite.rex.RexBuilder rb = inner.getCluster().getRexBuilder();
                java.util.List<org.apache.calcite.rex.RexNode> projects =
                    new java.util.ArrayList<>(fields.size() - 1);
                java.util.List<String> names = new java.util.ArrayList<>(fields.size() - 1);
                for (int i = 0; i < fields.size(); i++) {
                  if (i == hlIdx) continue;
                  projects.add(rb.makeInputRef(inner, i));
                  names.add(fields.get(i).getName());
                }
                inner =
                    org.apache.calcite.rel.logical.LogicalProject.create(
                        inner, java.util.Collections.emptyList(), projects, names);
              }
            }
            if (inner == walked.rel) return walked;
            return walked.clone(inner);
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            return super.visit(node).accept(shuttle);
          }
        });
  }

  /**
   * If the plan is a SIMPLE LINEAR chain (only Project/Filter/Sort nodes, exactly one input
   * each) terminating in a TableScan whose row-type ends with {@code _highlight}, AND no
   * operator in the chain references the {@code _highlight} column index, wrap the scan with
   * a {@code LogicalProject} that drops {@code _highlight} and re-clone every operator above
   * with the new input. Otherwise return the plan unchanged.
   *
   * <p>Returns the original plan unchanged on any failure mode (Join, Aggregate, Correlate,
   * UNNEST, RexInputRef referencing the dropped index, etc.). This makes the operation a
   * conservative best-effort optimization.
   */
  private static RelNode tryDeepStripHighlightInSimpleChain(RelNode root) {
    java.util.List<RelNode> chain = new java.util.ArrayList<>();
    RelNode cur = root;
    while (cur != null
        && (cur instanceof org.apache.calcite.rel.core.Project
            || cur instanceof org.apache.calcite.rel.core.Filter
            || cur instanceof org.apache.calcite.rel.core.Sort)
        && cur.getInputs().size() == 1) {
      chain.add(cur);
      cur = cur.getInput(0);
    }
    if (!(cur instanceof org.apache.calcite.rel.core.TableScan scan)) return root;
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields =
        scan.getRowType().getFieldList();
    if (fields.isEmpty()) return root;
    int last = fields.size() - 1;
    if (!"_highlight".equals(fields.get(last).getName())) return root;
    // Check no operator in the chain references the _highlight position.
    for (RelNode op : chain) {
      if (operatorReferencesIndex(op, last)) return root;
    }
    // Wrap scan with a Project that drops _highlight.
    org.apache.calcite.rex.RexBuilder rb = scan.getCluster().getRexBuilder();
    java.util.List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>(last);
    java.util.List<String> names = new java.util.ArrayList<>(last);
    for (int i = 0; i < last; i++) {
      projects.add(rb.makeInputRef(scan, i));
      names.add(fields.get(i).getName());
    }
    RelNode current =
        org.apache.calcite.rel.logical.LogicalProject.create(
            scan, java.util.Collections.emptyList(), projects, names);
    // Re-clone the chain bottom-up.
    for (int i = chain.size() - 1; i >= 0; i--) {
      RelNode op = chain.get(i);
      current = op.copy(op.getTraitSet(), java.util.List.of(current));
    }
    return current;
  }

  private static boolean operatorReferencesIndex(RelNode node, int idx) {
    boolean[] found = {false};
    org.apache.calcite.rex.RexVisitorImpl<Void> visitor =
        new org.apache.calcite.rex.RexVisitorImpl<>(true) {
          @Override
          public Void visitInputRef(org.apache.calcite.rex.RexInputRef ref) {
            if (ref.getIndex() == idx) found[0] = true;
            return null;
          }
        };
    if (node instanceof org.apache.calcite.rel.core.Filter f) {
      f.getCondition().accept(visitor);
    } else if (node instanceof org.apache.calcite.rel.core.Project p) {
      for (org.apache.calcite.rex.RexNode e : p.getProjects()) e.accept(visitor);
    } else if (node instanceof org.apache.calcite.rel.core.Sort s) {
      for (org.apache.calcite.rel.RelFieldCollation fc :
          s.getCollation().getFieldCollations()) {
        if (fc.getFieldIndex() == idx) return true;
      }
      if (s.fetch != null) s.fetch.accept(visitor);
      if (s.offset != null) s.offset.accept(visitor);
    }
    return found[0];
  }

  /**
   * Pattern aggregations return {@code ARRAY<MAP<VARCHAR, ANY>>}. The visitor's flatten step
   * extracts named values from each MAP via {@code CAST(ITEM(map, 'sample_logs') AS VARCHAR
   * ARRAY)}. Pre-round-trip the visitor uses a typed RexInputRef view so {@code ITEM} returns
   * {@code ARRAY<VARCHAR>} directly. After the round-trip the typed view is gone — {@code map}'s
   * declared value type is {@code ANY} so {@code ITEM(map, key)} returns {@code ANY}, and {@code
   * CAST(ANY AS ARRAY<...>)} hits Calcite's {@code RexToLixTranslator} assertion because {@code
   * source.getComponentType()} returns null.
   *
   * <p>Walk the round-tripped tree, find {@code CAST(ITEM(mapExpr, key) AS ARRAY<X>)} where the
   * ITEM source is a MAP whose value type is ANY, and replace the inner mapExpr with a typed view
   * (a {@link org.apache.calcite.rex.RexInputRef} or {@link org.apache.calcite.rex.RexFieldAccess}
   * re-typed as {@code MAP<key, ARRAY<X>>}). The ITEM call then returns {@code ARRAY<X>} and the
   * CAST becomes a no-op identity.
   */
  private static RelNode retypeItemForArrayCast(RelNode root) {
    org.apache.calcite.rex.RexBuilder rb0 = root.getCluster().getRexBuilder();
    org.apache.calcite.rex.RexShuttle shuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            if (visited.getKind() != org.apache.calcite.sql.SqlKind.CAST) return visited;
            org.apache.calcite.rel.type.RelDataType target = visited.getType();
            if (target.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.ARRAY) {
              return visited;
            }
            org.apache.calcite.rex.RexNode inner = visited.getOperands().get(0);
            if (!(inner instanceof org.apache.calcite.rex.RexCall itemCall)
                || itemCall.getKind() != org.apache.calcite.sql.SqlKind.ITEM) {
              return visited;
            }
            org.apache.calcite.rex.RexNode mapOperand = itemCall.getOperands().get(0);
            org.apache.calcite.rex.RexNode keyOperand = itemCall.getOperands().get(1);
            org.apache.calcite.rel.type.RelDataType mapType = mapOperand.getType();
            if (mapType.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.MAP) {
              return visited;
            }
            org.apache.calcite.rel.type.RelDataType valType = mapType.getValueType();
            if (valType == null
                || valType.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.ANY) {
              return visited;
            }
            // Build a re-typed map operand: same RexInputRef/FieldAccess but with
            // value type = target (ARRAY<X>). ITEM result type then becomes ARRAY<X>.
            org.apache.calcite.rex.RexBuilder rb = rb0;
            org.apache.calcite.rel.type.RelDataType newMapType =
                rb.getTypeFactory().createMapType(mapType.getKeyType(), target);
            // Use an explicit CAST so the validator agrees on the type. Then ITEM(map, key) on the
            // CAST value returns ARRAY<X> directly.
            org.apache.calcite.rex.RexNode retypedMap = rb.makeAbstractCast(newMapType, mapOperand);
            org.apache.calcite.rex.RexNode newItem =
                rb.makeCall(
                    target, itemCall.getOperator(), java.util.List.of(retypedMap, keyOperand));
            return newItem;
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            return super.visit(node).accept(shuttle);
          }
        });
  }

  /**
   * The Spark dialect unparses relevance functions' field MAPs as bare {@code MAP('k', v, ...)}
   * calls. After {@code SqlToRelConverter} reparses, the parser types the bare quoted strings as
   * {@code CHAR(n)} and {@code MAP_VALUE_CONSTRUCTOR}'s {@code leastRestrictive} widens them to
   * {@code CHAR(max-of-key-lengths)}, padding shorter keys with trailing spaces (a CHAR semantic).
   * That corrupts field names — {@code "Body"} becomes {@code "Body "} when paired with longer
   * {@code "Title"}.
   *
   * <p>Walk the round-tripped tree, find {@link RexCall}s whose operator is a multi-fields
   * relevance UDF ({@code simple_query_string} / {@code query_string} / {@code multi_match}), and
   * for each {@code MAP_VALUE_CONSTRUCTOR}/{@code MAP} operand whose value is itself a {@code MAP}
   * of {@code (CHAR, *)}, rebuild that inner map with VARCHAR-typed (untrimmed) key literals. This
   * restores the original visitor-time semantics without bypassing the round-trip.
   */
  private static RelNode unpadRelevanceMapKeys(RelNode root) {
    final java.util.Set<String> RELEVANCE_NAMES =
        java.util.Set.of("simple_query_string", "query_string", "multi_match");
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    org.apache.calcite.rel.type.RelDataType varcharType =
        rexBuilder.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    org.apache.calcite.rex.RexShuttle shuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            if (!RELEVANCE_NAMES.contains(
                visited.getOperator().getName().toLowerCase(java.util.Locale.ROOT))) {
              return visited;
            }
            // Each operand is a MAP('alias', value); we want to rebuild any inner MAP of fields.
            java.util.List<org.apache.calcite.rex.RexNode> newOperands =
                new java.util.ArrayList<>(visited.getOperands().size());
            boolean changed = false;
            for (org.apache.calcite.rex.RexNode op : visited.getOperands()) {
              org.apache.calcite.rex.RexNode rebuilt =
                  rebuildMapKeysVarchar(op, rexBuilder, varcharType);
              if (rebuilt != op) changed = true;
              newOperands.add(rebuilt);
            }
            if (!changed) return visited;
            return (org.apache.calcite.rex.RexCall)
                rexBuilder.makeCall(visited.getType(), visited.getOperator(), newOperands);
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            return super.visit(node).accept(shuttle);
          }
        });
  }

  private static org.apache.calcite.rex.RexNode rebuildMapKeysVarchar(
      org.apache.calcite.rex.RexNode node,
      org.apache.calcite.rex.RexBuilder rexBuilder,
      org.apache.calcite.rel.type.RelDataType varcharType) {
    if (!(node instanceof org.apache.calcite.rex.RexCall call)) return node;
    boolean isMap =
        call.getOperator() == SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR
            || call.getOperator() == org.apache.calcite.sql.fun.SqlLibraryOperators.MAP;
    if (!isMap) return node;
    java.util.List<org.apache.calcite.rex.RexNode> ops = call.getOperands();
    java.util.List<org.apache.calcite.rex.RexNode> rebuilt = new java.util.ArrayList<>(ops.size());
    boolean changed = false;
    for (int i = 0; i < ops.size(); i += 2) {
      org.apache.calcite.rex.RexNode key = ops.get(i);
      org.apache.calcite.rex.RexNode val = i + 1 < ops.size() ? ops.get(i + 1) : null;
      if (key instanceof org.apache.calcite.rex.RexLiteral lit
          && lit.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR
          && lit.getValue() instanceof org.apache.calcite.util.NlsString nls) {
        String trimmed = stripTrailingSpaces(nls.getValue());
        rebuilt.add(rexBuilder.makeLiteral(trimmed, varcharType, true));
        changed = true;
      } else {
        rebuilt.add(key);
      }
      if (val != null) {
        // Recurse into nested MAPs (relevance fields use a MAP-of-MAPs shape).
        org.apache.calcite.rex.RexNode rebuiltVal =
            rebuildMapKeysVarchar(val, rexBuilder, varcharType);
        if (rebuiltVal != val) changed = true;
        rebuilt.add(rebuiltVal);
      }
    }
    if (!changed) return node;
    return rexBuilder.makeCall(call.getOperator(), rebuilt);
  }

  private static String stripTrailingSpaces(String s) {
    int n = s.length();
    while (n > 0 && s.charAt(n - 1) == ' ') n--;
    return s.substring(0, n);
  }

  /**
   * RelToSql turns a {@code Correlate(LEFT)} into a {@code LATERAL} sub-query and the round-trip
   * re-builds it as {@code Correlate(INNER)} (Calcite's parser default). For streamstats-with-reset
   * that uses {@code Correlate.LEFT} to keep null-bucket rows, the INNER drop loses those rows.
   *
   * <p>Walk both plans pre-order and copy each {@code Correlate}'s {@code joinType} from the
   * original to the round-tripped position when shapes match. Mirrors {@link
   * #reattachAggregateHints}.
   */
  private static RelNode reattachCorrelateJoinTypes(RelNode original, RelNode roundTripped) {
    java.util.List<org.apache.calcite.rel.core.JoinRelType> origJoinTypes =
        collectCorrelateJoinTypes(original);
    if (origJoinTypes.isEmpty()) {
      return roundTripped;
    }
    int[] idx = {0};
    return roundTripped.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            if (visited instanceof org.apache.calcite.rel.core.Correlate c
                && idx[0] < origJoinTypes.size()) {
              org.apache.calcite.rel.core.JoinRelType orig = origJoinTypes.get(idx[0]++);
              if (c.getJoinType() != orig) {
                return c.copy(
                    c.getTraitSet(),
                    c.getLeft(),
                    c.getRight(),
                    c.getCorrelationId(),
                    c.getRequiredColumns(),
                    orig);
              }
            }
            return visited;
          }
        });
  }

  private static java.util.List<org.apache.calcite.rel.core.JoinRelType> collectCorrelateJoinTypes(
      RelNode node) {
    java.util.List<org.apache.calcite.rel.core.JoinRelType> types = new java.util.ArrayList<>();
    node.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode n) {
            if (n instanceof org.apache.calcite.rel.core.Correlate c) {
              types.add(c.getJoinType());
            }
            return super.visit(n);
          }
        });
    return types;
  }

  /**
   * The round-trip's {@code SqlToRelConverter} emits a {@code LogicalProject} for every
   * SELECT/sub-SELECT level — including identity star-projections like {@code SELECT * FROM (
   * SELECT * FROM t WHERE x) WHERE y}, which RelToSql produces from a chain of PPL filters.
   *
   * <p>Identity = the project's expression list is the same length as its input row-type, every
   * project expression is a {@link org.apache.calcite.rex.RexInputRef} pointing to the
   * corresponding input column index in order, and the field names match. Stripping these collapses
   * the round-tripped plan back toward the visitor's shape, which keeps explain output stable and
   * reduces redundant runtime materialisation.
   */
  private static RelNode stripIdentityProjects(RelNode root) {
    org.apache.calcite.rex.RexShuttle subqueryShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitSubQuery(
              org.apache.calcite.rex.RexSubQuery subQuery) {
            // Recurse into the subquery's plan so nested EXISTS/IN/Scalar subqueries also get
            // identity-Project stripping (they hold a RelNode plan in `rel` that the outer
            // RelHomogeneousShuttle does not visit).
            RelNode stripped = stripIdentityProjects(subQuery.rel);
            if (stripped == subQuery.rel) {
              return super.visitSubQuery(subQuery);
            }
            return subQuery.clone(stripped);
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            // Rewrite any RexSubQuery references first — Filter/Project conditions can carry
            // them and we want stripping inside their nested plans too.
            RelNode visited = super.visit(node).accept(subqueryShuttle);
            if (!(visited instanceof org.apache.calcite.rel.core.Project project)) {
              return visited;
            }
            org.apache.calcite.rel.type.RelDataType inputType = project.getInput().getRowType();
            org.apache.calcite.rel.type.RelDataType projectType = project.getRowType();
            if (inputType.getFieldCount() != projectType.getFieldCount()) {
              return visited;
            }
            for (int i = 0; i < project.getProjects().size(); i++) {
              org.apache.calcite.rex.RexNode e = project.getProjects().get(i);
              if (!(e instanceof org.apache.calcite.rex.RexInputRef ref) || ref.getIndex() != i) {
                return visited;
              }
              if (!inputType
                  .getFieldList()
                  .get(i)
                  .getName()
                  .equals(projectType.getFieldList().get(i).getName())) {
                return visited;
              }
            }
            return project.getInput();
          }
        });
  }

  private static boolean containsGraphLookup(RelNode root) {
    boolean[] found = {false};
    root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            if (node instanceof org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup) {
              found[0] = true;
              return node;
            }
            return super.visit(node);
          }
        });
    return found[0];
  }

  /**
   * Walk {@code original} and {@code roundTripped} in pre-order and copy any non-empty Aggregate
   * hints from positions in {@code original} to corresponding positions in {@code roundTripped}.
   * Matches by node-class identity (Aggregate vs Aggregate) and traversal index. If shapes diverge
   * mid-walk, stop re-attaching to avoid cross-contamination.
   */
  private static RelNode reattachAggregateHints(RelNode original, RelNode roundTripped) {
    java.util.List<org.apache.calcite.rel.hint.RelHint> origHints = collectAggregateHints(original);
    if (origHints.isEmpty() || origHints.stream().allMatch(h -> h == null)) return roundTripped;
    // Ensure the cluster has a HintStrategyTable that registers AGG_ARGS — Aggregate.withHints
    // otherwise drops unknown hints silently.
    if (roundTripped.getCluster().getHintStrategies()
        == org.apache.calcite.rel.hint.HintStrategyTable.EMPTY) {
      roundTripped
          .getCluster()
          .setHintStrategies(org.opensearch.sql.calcite.utils.PPLHintUtils.getHintStrategyTable());
    }
    int[] idx = {0};
    return roundTripped.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            if (visited instanceof org.apache.calcite.rel.core.Aggregate agg
                && idx[0] < origHints.size()) {
              org.apache.calcite.rel.hint.RelHint hint = origHints.get(idx[0]++);
              if (hint != null && agg.getHints().isEmpty()) {
                return agg.withHints(java.util.List.of(hint));
              }
            }
            return visited;
          }
        });
  }

  private static java.util.List<org.apache.calcite.rel.hint.RelHint> collectAggregateHints(
      RelNode node) {
    java.util.List<org.apache.calcite.rel.hint.RelHint> hints = new java.util.ArrayList<>();
    node.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode n) {
            if (n instanceof org.apache.calcite.rel.core.Aggregate agg) {
              hints.add(agg.getHints().isEmpty() ? null : agg.getHints().get(0));
            }
            return super.visit(n);
          }
        });
    return hints;
  }

  static String relToSql(RelNode rel) {
    RelNode prepared = wrapFloatLiteralsForRoundTrip(rel);
    prepared = wrapVarcharCaseBranchesForRoundTrip(prepared);
    prepared = wrapVarcharLiteralsBelowUnionForRoundTrip(prepared);
    prepared = materialiseEmptyValuesForRoundTrip(prepared);
    prepared = liftWindowedAggsAboveAggregateGroupByForRoundTrip(prepared);
    prepared = isolateSortInputForRoundTrip(prepared);
    RelToSqlConverter converter =
        new org.apache.calcite.rel.rel2sql.OpenSearchRelToSqlConverter(
            OpenSearchSparkSqlDialect.DEFAULT);
    SqlImplementor.Result result = converter.visitRoot(prepared);
    SqlNode sqlNode = result.asStatement();
    sqlNode = stripUnusedAsOverJoin(sqlNode);
    return sqlNode.toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
  }

  /**
   * If a Project below an Aggregate computes a windowed aggregate (e.g. {@code MAX(x) OVER ()}) and
   * that windowed expression appears as part of an Aggregate group-by key, the SqlNodePipeline
   * round-trip rejects with "Windowed aggregate expression is illegal in GROUP BY clause" — the
   * unparser inlines the windowed expression in the GROUP BY clause and Calcite's strict validator
   * forbids that.
   *
   * <p>Pre-process the plan: when an Aggregate sits directly on a Project containing windowed
   * aggregates, materialise those windowed-agg expressions in an extra sub-Project below, so the
   * outer Project (and any subsequent Aggregate group-by) references the windowed result via a
   * column reference instead of inlining the windowed expression.
   */
  private static RelNode liftWindowedAggsAboveAggregateGroupByForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            // Lift any Project that contains windowed aggregates inline. Without lifting, the
            // unparser inlines the windowed expression into SELECT (and any Aggregate group-by
            // or Sort key referencing it) and the SqlValidator rejects "Windowed aggregate
            // expression is illegal in GROUP BY clause", or the round-trip generates nested
            // aggregates (WIDTH_BUCKET-of-WIDTH_BUCKET) when the bin'd column is referenced by
            // a downstream Sort.
            if (visited instanceof org.apache.calcite.rel.core.Project project
                && project.getProjects().stream().anyMatch(SqlNodePipeline::containsWindowedAgg)) {
              return liftProjectWindowedAggs(project, rexBuilder);
            }
            return visited;
          }
        });
  }

  private static boolean containsWindowedAgg(org.apache.calcite.rex.RexNode expr) {
    boolean[] found = {false};
    expr.accept(
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitOver(org.apache.calcite.rex.RexOver over) {
            found[0] = true;
            return null;
          }
        });
    return found[0];
  }

  private static RelNode liftProjectWindowedAggs(
      org.apache.calcite.rel.core.Project outerProject,
      org.apache.calcite.rex.RexBuilder rexBuilder) {
    RelNode origInput = outerProject.getInput();
    java.util.List<org.apache.calcite.rex.RexNode> origInputProjects = new java.util.ArrayList<>();
    java.util.List<String> origInputNames = new java.util.ArrayList<>();
    java.util.List<String> origNames = origInput.getRowType().getFieldNames();
    for (int i = 0; i < origInput.getRowType().getFieldCount(); i++) {
      origInputProjects.add(rexBuilder.makeInputRef(origInput, i));
      origInputNames.add(origNames.get(i));
    }
    // For each windowed-agg sub-expression in the outer Project, add it to the sub-Project and
    // remember its new column index.
    java.util.Map<String, Integer> windowedToCol = new java.util.HashMap<>();
    java.util.List<org.apache.calcite.rex.RexOver> uniqueOvers = new java.util.ArrayList<>();
    for (org.apache.calcite.rex.RexNode expr : outerProject.getProjects()) {
      collectWindowedAggs(expr, uniqueOvers);
    }
    if (uniqueOvers.isEmpty()) {
      return outerProject;
    }
    int aliasCounter = 0;
    for (org.apache.calcite.rex.RexOver over : uniqueOvers) {
      String key = over.toString();
      if (windowedToCol.containsKey(key)) continue;
      windowedToCol.put(key, origInputProjects.size());
      origInputProjects.add(over);
      origInputNames.add("__win_" + (aliasCounter++));
    }
    RelNode newInput =
        org.apache.calcite.rel.logical.LogicalProject.create(
            origInput,
            java.util.Collections.emptyList(),
            origInputProjects,
            origInputNames,
            java.util.Set.of());
    // Force a derived-table boundary so RelToSqlConverter cannot merge this Project into the
    // outer Project. Without this, Calcite inlines the windowed-agg references back into the
    // outer Project's SQL output, which the SqlValidator round-trip rejects in the GROUP BY.
    newInput =
        org.apache.calcite.rel.logical.LogicalFilter.create(newInput, rexBuilder.makeLiteral(true));
    // Rewrite the outer Project's expressions so windowed-agg subexpressions become column refs.
    java.util.List<org.apache.calcite.rex.RexNode> newOuterExprs = new java.util.ArrayList<>();
    for (org.apache.calcite.rex.RexNode expr : outerProject.getProjects()) {
      newOuterExprs.add(rewriteWindowedAggsAsColumnRefs(expr, windowedToCol, newInput, rexBuilder));
    }
    return outerProject.copy(
        outerProject.getTraitSet(), newInput, newOuterExprs, outerProject.getRowType());
  }

  private static void collectWindowedAggs(
      org.apache.calcite.rex.RexNode expr, java.util.List<org.apache.calcite.rex.RexOver> out) {
    expr.accept(
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitOver(org.apache.calcite.rex.RexOver over) {
            // Only collect "leaf" windowed aggs — the ones whose direct sub-expressions are not
            // themselves windowed. (Nested OVERs are illegal anyway.) Use exact-string dedup.
            for (org.apache.calcite.rex.RexOver existing : out) {
              if (existing.toString().equals(over.toString())) return null;
            }
            out.add(over);
            return null;
          }
        });
  }

  private static org.apache.calcite.rex.RexNode rewriteWindowedAggsAsColumnRefs(
      org.apache.calcite.rex.RexNode expr,
      java.util.Map<String, Integer> windowedToCol,
      RelNode newInput,
      org.apache.calcite.rex.RexBuilder rexBuilder) {
    return expr.accept(
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitOver(org.apache.calcite.rex.RexOver over) {
            Integer colIdx = windowedToCol.get(over.toString());
            if (colIdx == null) return over;
            return rexBuilder.makeInputRef(newInput, colIdx);
          }
        });
  }

  /**
   * Wrap VARCHAR/CHAR {@link org.apache.calcite.rex.RexLiteral} expressions in any {@link
   * org.apache.calcite.rel.core.Project} that feeds a {@link org.apache.calcite.rel.core.Union}.
   * After RelToSql unparses the UNION's input projections, bare quoted strings re-parse as {@code
   * CHAR(N)}. The UNION's leastRestrictive type then widens to {@code CHAR(max-of-branch-lengths)}
   * and SQL CHAR semantics pad shorter values with trailing spaces — so {@code "Illinois"} appears
   * as {@code "Illinois "}. Wrapping each VARCHAR/CHAR literal in {@code CAST(... AS VARCHAR)}
   * forces the parser to type them as VARCHAR, keeping the UNION result VARCHAR.
   */
  private static RelNode wrapVarcharLiteralsBelowUnionForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            if (!(visited instanceof org.apache.calcite.rel.core.Union)) {
              return visited;
            }
            java.util.List<RelNode> wrappedInputs =
                new java.util.ArrayList<>(visited.getInputs().size());
            boolean changed = false;
            for (RelNode input : visited.getInputs()) {
              RelNode wrapped = wrapVarcharLiteralsInProject(input, rexBuilder);
              wrappedInputs.add(wrapped);
              if (wrapped != input) changed = true;
            }
            if (!changed) return visited;
            return visited.copy(visited.getTraitSet(), wrappedInputs);
          }
        });
  }

  private static RelNode wrapVarcharLiteralsInProject(
      RelNode input, org.apache.calcite.rex.RexBuilder rexBuilder) {
    if (!(input instanceof org.apache.calcite.rel.core.Project project)) {
      return input;
    }
    java.util.List<org.apache.calcite.rex.RexNode> projects = project.getProjects();
    java.util.List<org.apache.calcite.rex.RexNode> rewritten =
        new java.util.ArrayList<>(projects.size());
    boolean changed = false;
    for (org.apache.calcite.rex.RexNode expr : projects) {
      if (isVarcharLiteral(expr)) {
        rewritten.add(rexBuilder.makeAbstractCast(expr.getType(), expr));
        changed = true;
      } else {
        rewritten.add(expr);
      }
    }
    if (!changed) return input;
    return project.copy(project.getTraitSet(), project.getInput(), rewritten, project.getRowType());
  }

  /**
   * When a {@link org.apache.calcite.rel.core.Sort} is fed by a {@link
   * org.apache.calcite.rel.core.Project} that overrides an input column with a same-named output of
   * a different type (e.g. PPL {@code bin field span=N} produces {@code Project(field =
   * SPAN_BUCKET(field, N))} where input {@code field} is INTEGER and output is VARCHAR), the
   * unparser collapses Sort+Project into {@code SELECT SPAN_BUCKET(field) AS field FROM t ORDER BY
   * SPAN_BUCKET(field)}. After re-parse, ORDER BY's {@code field} reference resolves to the SELECT
   * alias (now VARCHAR), and the inlined SPAN_BUCKET on a VARCHAR alias trips the validator with
   * "Cannot apply 'SPAN_BUCKET' to arguments of type SPAN_BUCKET(<VARCHAR>)".
   *
   * <p>Force a derived-table boundary by inserting a {@code Filter(true)} between Sort and the
   * type-overriding Project: {@code Sort(...) → Filter(true) → Project(...)} unparses as {@code
   * SELECT * FROM (SELECT SPAN_BUCKET(...) AS field FROM t WHERE TRUE) ORDER BY field}. The ORDER
   * BY now references the outer alias (VARCHAR), which is OK because we sort by alias directly
   * rather than inlining the SPAN_BUCKET expression.
   */
  private static RelNode isolateSortInputForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            if (!(visited instanceof org.apache.calcite.rel.core.Sort sort)) {
              return visited;
            }
            RelNode sortInput = sort.getInput();
            if (!(sortInput instanceof org.apache.calcite.rel.core.Project project)) {
              return visited;
            }
            if (!projectOverridesColumnType(project) && !projectShadowsInputName(project)) {
              return visited;
            }
            // Wrap project in Filter(true) to force a sub-SELECT boundary on unparse. This
            // applies when the project either overrides the input column's type (e.g. bin
            // outputs a VARCHAR over a TIMESTAMP input column) OR shadows an input column name
            // with a non-input-ref expression (e.g. bin's DATE_FORMAT(...) AS @timestamp). The
            // latter case prevents Calcite's RelToSqlConverter from merging Project+Sort into
            // a single SELECT and inlining the bin expression in ORDER BY, which would cause
            // alias-shadowing during re-parse: an inner @timestamp reference inside the
            // inlined ORDER BY expression would resolve to the SELECT alias (the formatted
            // string output) instead of the FROM column, blowing up at runtime.
            RelNode wrappedProject =
                org.apache.calcite.rel.logical.LogicalFilter.create(
                    project, rexBuilder.makeLiteral(true));
            return sort.copy(sort.getTraitSet(), java.util.List.of(wrappedProject));
          }
        });
  }

  /**
   * True if any project expression has the same output name as an input column AND is not a
   * trivial {@link org.apache.calcite.rex.RexInputRef} for that column. This catches the bin
   * pattern where {@code DATE_FORMAT(...) AS @timestamp} shadows the input {@code @timestamp}
   * column.
   */
  private static boolean projectShadowsInputName(org.apache.calcite.rel.core.Project project) {
    java.util.List<String> inputNames = project.getInput().getRowType().getFieldNames();
    java.util.Set<String> inputSet = new java.util.HashSet<>(inputNames);
    java.util.List<String> outputNames = project.getRowType().getFieldNames();
    java.util.List<org.apache.calcite.rex.RexNode> exprs = project.getProjects();
    for (int i = 0; i < outputNames.size(); i++) {
      String outName = outputNames.get(i);
      org.apache.calcite.rex.RexNode expr = exprs.get(i);
      if (!inputSet.contains(outName)) continue;
      if (expr instanceof org.apache.calcite.rex.RexInputRef ref
          && ref.getIndex() < inputNames.size()
          && inputNames.get(ref.getIndex()).equals(outName)) {
        continue; // identity reference — not shadowing
      }
      return true;
    }
    return false;
  }

  private static boolean projectOverridesColumnType(org.apache.calcite.rel.core.Project project) {
    org.apache.calcite.rel.type.RelDataType inputType = project.getInput().getRowType();
    org.apache.calcite.rel.type.RelDataType outputType = project.getRowType();
    java.util.Map<String, org.apache.calcite.sql.type.SqlTypeName> inputByName =
        new java.util.HashMap<>();
    for (org.apache.calcite.rel.type.RelDataTypeField f : inputType.getFieldList()) {
      inputByName.put(f.getName(), f.getType().getSqlTypeName());
    }
    for (org.apache.calcite.rel.type.RelDataTypeField outF : outputType.getFieldList()) {
      org.apache.calcite.sql.type.SqlTypeName inSql = inputByName.get(outF.getName());
      if (inSql != null && inSql != outF.getType().getSqlTypeName()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Strip {@code AS(SqlJoin, alias)} wrappers from the FROM clause when the {@code alias} is not
   * referenced anywhere in the enclosing SELECT.
   *
   * <p>Calcite's {@link RelToSqlConverter} wraps the outermost FROM-position join with a generated
   * alias (e.g. {@code (... ) t11}). The Spark dialect's unparse adds parens around the JOIN
   * because of operator precedence — the resulting {@code FROM ((SELECT ...) t6 CROSS JOIN ...)
   * t11} is rejected by the Babel parser ({@code TableRef3}: "Join expression encountered in
   * illegal context"). When {@code t11} is unused (the SELECT/WHERE references inner aliases like
   * {@code t6.state} only), dropping the AS leaves the bare {@code SqlJoin} which the unparser
   * writes without surrounding parens.
   *
   * <p>If the alias is referenced (e.g. {@code SELECT t11.col FROM ...}), the AS is preserved — the
   * round-trip still fails for that shape, but it's a smaller surface than blanket stripping.
   */
  private static SqlNode stripUnusedAsOverJoin(SqlNode root) {
    if (!(root instanceof org.apache.calcite.sql.SqlSelect select)) {
      return root;
    }
    SqlNode from = select.getFrom();
    if (from == null) {
      return root;
    }
    SqlNode stripped = stripUnusedAsOverJoinInFrom(from, select);
    if (stripped != from) {
      select.setFrom(stripped);
    }
    return root;
  }

  private static SqlNode stripUnusedAsOverJoinInFrom(
      SqlNode from, org.apache.calcite.sql.SqlSelect outer) {
    if (from instanceof org.apache.calcite.sql.SqlBasicCall basic
        && basic.getOperator() == org.apache.calcite.sql.fun.SqlStdOperatorTable.AS
        && basic.getOperandList().size() == 2
        && basic.getOperandList().get(0) instanceof org.apache.calcite.sql.SqlJoin innerJoin
        && basic.getOperandList().get(1) instanceof org.apache.calcite.sql.SqlIdentifier alias) {
      String aliasName = alias.getSimple();
      if (!aliasReferenced(outer, aliasName, basic)) {
        return innerJoin;
      }
    }
    return from;
  }

  private static boolean aliasReferenced(SqlNode root, String aliasName, SqlNode skip) {
    final boolean[] found = {false};
    org.apache.calcite.sql.util.SqlBasicVisitor<Void> visitor =
        new org.apache.calcite.sql.util.SqlBasicVisitor<>() {
          @Override
          public Void visit(org.apache.calcite.sql.SqlIdentifier id) {
            if (id.names.size() >= 2 && aliasName.equalsIgnoreCase(id.names.get(0))) {
              found[0] = true;
            }
            return null;
          }

          @Override
          public Void visit(org.apache.calcite.sql.SqlCall call) {
            if (call == skip || found[0]) {
              return null;
            }
            return super.visit(call);
          }
        };
    root.accept(visitor);
    return found[0];
  }

  /**
   * Replace every empty {@link org.apache.calcite.rel.logical.LogicalValues} node (the shape that
   * {@code RelBuilder.filter(falseLiteral)} collapses to) with a one-row {@code LogicalValues}
   * carrying typed {@code CAST(NULL AS T)} literals plus a {@code Filter(false)}. Without this,
   * {@link RelToSqlConverter#visit(org.apache.calcite.rel.core.Values)} unparses each cell as the
   * bare keyword {@code NULL} — re-parsing then types every column as {@code SqlTypeName.NULL},
   * which collapses {@code SUM(BIGINT)} into {@code SUM(NULL)}. The validator's least-restrictive
   * coercion widens that to {@code DECIMAL(38, 19)} and the column reaches the response formatter
   * as {@code double} where the user asked for {@code bigint}. Materialising typed casts preserves
   * each column's source type through the round trip.
   */
  private static RelNode materialiseEmptyValuesForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            if (!(visited instanceof org.apache.calcite.rel.core.Values values)) {
              return visited;
            }
            if (!values.getTuples().isEmpty()) {
              return visited;
            }
            org.apache.calcite.rel.type.RelDataType rowType = values.getRowType();
            com.google.common.collect.ImmutableList.Builder<
                    com.google.common.collect.ImmutableList<org.apache.calcite.rex.RexLiteral>>
                tupleBuilder = com.google.common.collect.ImmutableList.builder();
            com.google.common.collect.ImmutableList.Builder<org.apache.calcite.rex.RexLiteral>
                rowBuilder = com.google.common.collect.ImmutableList.builder();
            for (org.apache.calcite.rel.type.RelDataTypeField f : rowType.getFieldList()) {
              rowBuilder.add(rexBuilder.makeNullLiteral(f.getType()));
            }
            tupleBuilder.add(rowBuilder.build());
            RelNode oneRow =
                org.apache.calcite.rel.logical.LogicalValues.create(
                    values.getCluster(), rowType, tupleBuilder.build());
            // Wrap each column with CAST(NULL AS T) so the unparser emits typed casts rather than
            // bare NULL literals; only typed casts survive parser → validator with their type.
            java.util.List<org.apache.calcite.rex.RexNode> projects =
                new java.util.ArrayList<>(rowType.getFieldCount());
            java.util.List<String> names = new java.util.ArrayList<>(rowType.getFieldCount());
            // Skip columns whose type contains ANY (e.g. MAP<VARCHAR, ANY> for the synthetic
            // _highlight column) — SqlTypeUtil.convertTypeToSpec rejects ANY and the entire
            // CAST list fails to unparse. The empty Values has no rows so dropping the
            // column from the materialised representation is harmless; downstream operators
            // referencing those columns are filtered by Filter(false) anyway.
            org.apache.calcite.rel.type.RelDataTypeFactory.Builder narrowSchema =
                values.getCluster().getTypeFactory().builder();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
              org.apache.calcite.rel.type.RelDataTypeField f = rowType.getFieldList().get(i);
              if (containsAnyType(f.getType())) {
                continue;
              }
              org.apache.calcite.rex.RexNode nullLit = rexBuilder.makeNullLiteral(f.getType());
              projects.add(rexBuilder.makeAbstractCast(f.getType(), nullLit));
              names.add(f.getName());
              narrowSchema.add(f.getName(), f.getType());
            }
            // Rebuild the one-row Values with the narrowed schema (mirrors the project list).
            com.google.common.collect.ImmutableList.Builder<org.apache.calcite.rex.RexLiteral>
                narrowRowBuilder = com.google.common.collect.ImmutableList.builder();
            for (org.apache.calcite.rel.type.RelDataTypeField f :
                narrowSchema.build().getFieldList()) {
              narrowRowBuilder.add(rexBuilder.makeNullLiteral(f.getType()));
            }
            oneRow =
                org.apache.calcite.rel.logical.LogicalValues.create(
                    values.getCluster(),
                    narrowSchema.build(),
                    com.google.common.collect.ImmutableList.of(narrowRowBuilder.build()));
            RelNode projected =
                org.apache.calcite.rel.logical.LogicalProject.create(
                    oneRow, java.util.Collections.emptyList(), projects, names, java.util.Set.of());
            return org.apache.calcite.rel.logical.LogicalFilter.create(
                projected, rexBuilder.makeLiteral(false));
          }
        });
  }

  /**
   * Recursively check whether the given type or any of its component types is {@link
   * org.apache.calcite.sql.type.SqlTypeName#ANY}. Used to skip {@code CAST(NULL AS T)} on column
   * types that {@code SqlTypeUtil.convertTypeToSpec} cannot serialise (e.g. {@code MAP<VARCHAR,
   * ANY>} for the synthetic {@code _highlight} column).
   */
  private static boolean containsAnyType(org.apache.calcite.rel.type.RelDataType t) {
    if (t.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.ANY) {
      return true;
    }
    if (t.getComponentType() != null && containsAnyType(t.getComponentType())) {
      return true;
    }
    if (t.getKeyType() != null && containsAnyType(t.getKeyType())) {
      return true;
    }
    if (t.getValueType() != null && containsAnyType(t.getValueType())) {
      return true;
    }
    return false;
  }

  /**
   * Wrap each VARCHAR/CHAR {@link org.apache.calcite.rex.RexLiteral} that appears as a direct
   * branch (THEN/ELSE) of a {@code CASE} call with an explicit {@code CAST(... AS VARCHAR)}.
   *
   * <p>The unparser writes a VARCHAR literal as a bare quoted string (e.g. {@code 'old'}). The
   * Babel parser re-types bare quoted strings as {@code CHAR(N)}. When such literals appear as
   * branches of a {@code CASE}, Calcite's least-restrictive type computation widens the result to
   * {@code CHAR(max-of-branch-lengths)}, and SQL CHAR semantics pad shorter values with trailing
   * spaces — so {@code case(..., 'adult', ...)} returns {@code "adult "} instead of {@code
   * "adult"}. Wrapping the branches in {@code CAST(... AS VARCHAR)} forces the parser to type them
   * as VARCHAR; the CASE result type stays VARCHAR and the values keep their actual length.
   *
   * <p>The wrapping is scoped to CASE branches because broader wrapping (every VARCHAR literal)
   * defeats OpenSearch pushdown analyzers that pattern-match on {@code RexInputRef = RexLiteral}
   * pairs. Comparisons like {@code name = 'Jake'} keep their bare literal RHS and continue to push
   * down.
   */
  private static RelNode wrapVarcharCaseBranchesForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    org.apache.calcite.rex.RexShuttle shuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexNode visited = super.visitCall(call);
            if (!(visited instanceof org.apache.calcite.rex.RexCall outer)) {
              return visited;
            }
            if (outer.getOperator() == SqlStdOperatorTable.CASE) {
              return wrapCaseBranches(outer, rexBuilder);
            }
            // PPL_ARRAY/array constructor — leastRestrictive over CHAR(N) operands widens to
            // CHAR(max) padded; wrap each VARCHAR/CHAR operand to keep VARCHAR semantics.
            if ("PPL_ARRAY".equals(outer.getOperator().getName())
                || "ARRAY".equals(outer.getOperator().getName())) {
              return wrapAllVarcharOperands(outer, rexBuilder);
            }
            return outer;
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            return visited.accept(shuttle);
          }
        });
  }

  private static org.apache.calcite.rex.RexNode wrapCaseBranches(
      org.apache.calcite.rex.RexCall outer, org.apache.calcite.rex.RexBuilder rexBuilder) {
    // CASE operand layout: [whenA, thenA, whenB, thenB, ..., elseExpr]
    // Wrap odd-index branches (the THENs) and the trailing ELSE.
    java.util.List<org.apache.calcite.rex.RexNode> ops = outer.getOperands();
    java.util.List<org.apache.calcite.rex.RexNode> rewritten =
        new java.util.ArrayList<>(ops.size());
    boolean changed = false;
    for (int i = 0; i < ops.size(); i++) {
      org.apache.calcite.rex.RexNode op = ops.get(i);
      boolean isBranch = (i % 2 == 1) || (i == ops.size() - 1 && i % 2 == 0);
      if (isBranch && isVarcharLiteral(op)) {
        rewritten.add(rexBuilder.makeAbstractCast(op.getType(), op));
        changed = true;
      } else {
        rewritten.add(op);
      }
    }
    if (!changed) {
      return outer;
    }
    return rexBuilder.makeCall(outer.getType(), outer.getOperator(), rewritten);
  }

  private static org.apache.calcite.rex.RexNode wrapAllVarcharOperands(
      org.apache.calcite.rex.RexCall outer, org.apache.calcite.rex.RexBuilder rexBuilder) {
    java.util.List<org.apache.calcite.rex.RexNode> ops = outer.getOperands();
    java.util.List<org.apache.calcite.rex.RexNode> rewritten =
        new java.util.ArrayList<>(ops.size());
    boolean changed = false;
    for (org.apache.calcite.rex.RexNode op : ops) {
      if (isVarcharLiteral(op)) {
        rewritten.add(rexBuilder.makeAbstractCast(op.getType(), op));
        changed = true;
      } else {
        rewritten.add(op);
      }
    }
    if (!changed) {
      return outer;
    }
    return rexBuilder.makeCall(outer.getType(), outer.getOperator(), rewritten);
  }

  private static boolean isVarcharLiteral(org.apache.calcite.rex.RexNode op) {
    return op instanceof org.apache.calcite.rex.RexLiteral
        && (op.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
            || op.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR);
  }

  /**
   * Wrap every {@code FLOAT}/{@code REAL} (single-precision) {@link
   * org.apache.calcite.rex.RexLiteral} in an explicit {@code CAST(... AS REAL)}.
   *
   * <p>SQL textual literal syntax has no marker for single-precision floats: the unparser writes a
   * FLOAT/REAL RexLiteral as the same bare numeric form as a DOUBLE one (e.g. {@code 6E-2}).
   * Calcite's parser then types every exponent-bearing numeric literal as DOUBLE, so the
   * round-tripped plan reports a DOUBLE column where the visitor produced FLOAT — contradicting
   * PPL's rule that {@code FLOAT - FLOAT} stays FLOAT. The CAST forces the unparser to emit {@code
   * CAST(6E-2 AS REAL)}, whose target type the parser reads explicitly. Both {@link
   * org.apache.calcite.sql.type.SqlTypeName#FLOAT} and {@link
   * org.apache.calcite.sql.type.SqlTypeName#REAL} are wrapped because Calcite's {@code
   * RexBuilder.makeCast} for {@code DECIMAL → REAL} folds the literal to a REAL-typed RexLiteral,
   * not a FLOAT-typed one. {@code makeAbstractCast} preserves the CAST as a Rex node (a plain
   * {@code makeCast} would constant-fold it back into a literal).
   */
  private static RelNode wrapFloatLiteralsForRoundTrip(RelNode root) {
    org.apache.calcite.rex.RexBuilder rexBuilder = root.getCluster().getRexBuilder();
    org.apache.calcite.rex.RexShuttle shuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitLiteral(
              org.apache.calcite.rex.RexLiteral literal) {
            org.apache.calcite.sql.type.SqlTypeName n = literal.getType().getSqlTypeName();
            if (n == org.apache.calcite.sql.type.SqlTypeName.FLOAT
                || n == org.apache.calcite.sql.type.SqlTypeName.REAL) {
              return rexBuilder.makeAbstractCast(literal.getType(), literal);
            }
            return literal;
          }
        };
    return root.accept(
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode node) {
            RelNode visited = super.visit(node);
            return visited.accept(shuttle);
          }
        });
  }

  static RelNode sqlToRel(String sql, CalcitePlanContext context) {
    FrameworkConfig fc = context.config;
    SchemaPlus defaultSchema = fc.getDefaultSchema();
    OpenSearchTypeFactory tf = OpenSearchTypeFactory.TYPE_FACTORY;

    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig connConfig = new CalciteConnectionConfigImpl(props);

    CalciteSchema rootSchema = CalciteSchema.from(defaultSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, Collections.emptyList(), tf, connConfig);

    SqlOperatorTable operatorTable = buildOperatorTable();

    // Match the Spark dialect's NullCollation (LOW): NULLS FIRST is the default for ASC
    // and NULLS LAST is the default for DESC. PPL's visitor sets the same defaults via
    // RelBuilder.nullsFirst()/nullsLast(), and Spark's RelToSqlConverter omits the explicit
    // NULLS FIRST/NULLS LAST keywords when they match the dialect default. Without this
    // override, the validator's default (HIGH) flips the null direction after the round-trip
    // and Sort tests with default null ordering — including the implicit nulls-first on ASC
    // applied by the visitor — observe inverted results.
    SqlValidator validator =
        SqlValidatorUtil.newValidator(
            operatorTable,
            catalogReader,
            tf,
            SqlValidator.Config.DEFAULT
                .withTypeCoercionFactory(PPLTypeCoercion.FACTORY)
                .withDefaultNullCollation(NullCollation.LOW)
                .withIdentifierExpansion(true)
                // STRICT_2003: GROUP BY/ORDER BY refer to FROM-clause columns, not SELECT aliases.
                // RelToSql may emit SELECT lists where the same name appears as both an output
                // alias (e.g. `bin age`'s VARCHAR result aliased AS `age`) and an input column
                // (the unmodified BIGINT `age` from the FROM clause). The default conformance
                // resolves bare GROUP BY/ORDER BY references to the SELECT alias, which flips
                // the type and breaks downstream validation (DIVIDE(VARCHAR, DOUBLE)). Strict
                // 2003 forces FROM-side resolution which matches the original RelNode plan.
                .withSqlConformance(
                    org.apache.calcite.sql.validate.SqlConformanceEnum.STRICT_2003));

    SqlParser.Config parserConfig =
        fc.getParserConfig()
            .withQuoting(Quoting.BACK_TICK)
            .withParserFactory(SqlBabelParserImpl.FACTORY);
    SqlNode parsed;
    try {
      parsed = SqlParser.create(sql, parserConfig).parseQuery();
    } catch (org.apache.calcite.sql.parser.SqlParseException e) {
      throw new RuntimeException(
          "SqlNodePipeline: failed to re-parse generated SQL\n----\n" + sql + "\n----", e);
    }
    SqlNode validated;
    try {
      validated = validator.validate(parsed);
    } catch (org.apache.calcite.runtime.CalciteContextException e) {
      // Type-mismatch and similar user-side errors surface as CalciteContextException from
      // SqlValidator. The REST layer maps QueryEngineException to HTTP 400 (vs the default 500
      // for opaque Throwables); rewrap so the user gets a proper status code rather than an
      // "internal error".
      throw new org.opensearch.sql.exception.ExpressionEvaluationException(e.getMessage(), e);
    }

    // withRemoveSortInSubQuery(false): Calcite's default removes ORDER BY clauses inside derived
    // tables (sub-SELECTs without LIMIT) because they are technically redundant in standard SQL.
    // PPL plans rely on inner Sort+Project shapes to feed an outer windowed-aggregate (e.g.
    // `trendline sort - SAL sma(2, SAL)` produces Project(window) → Filter → Sort(SAL DESC)).
    // Stripping the inner Sort breaks ordering-dependent windows; preserve the Sort.
    SqlToRelConverter sqlToRel =
        new SqlToRelConverter(
            (rowType, queryString, schemaPath, viewPath) -> null,
            validator,
            catalogReader,
            context.relBuilder.getCluster(),
            new PplConvertletTable(),
            SqlToRelConverter.config().withTrimUnusedFields(false).withRemoveSortInSubQuery(false));

    RelRoot root = sqlToRel.convertQuery(validated, false, true);
    return root.project();
  }
}
