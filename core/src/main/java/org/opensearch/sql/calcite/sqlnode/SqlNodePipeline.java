/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.Collections;
import java.util.List;
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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
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
    SqlOperatorTable lib =
        org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY,
            org.apache.calcite.sql.fun.SqlLibrary.SPARK,
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
   */
  public static RelNode revalidate(RelNode original, CalcitePlanContext context) {
    String sql = relToSql(original);
    return sqlToRel(sql, context);
  }

  static String relToSql(RelNode rel) {
    RelNode prepared = wrapFloatLiteralsForRoundTrip(rel);
    RelToSqlConverter converter = new RelToSqlConverter(OpenSearchSparkSqlDialect.DEFAULT);
    SqlImplementor.Result result = converter.visitRoot(prepared);
    SqlNode sqlNode = result.asStatement();
    return sqlNode.toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
  }

  /**
   * Wrap every {@code FLOAT}/{@code REAL} (single-precision) {@link
   * org.apache.calcite.rex.RexLiteral} in an explicit {@code CAST(... AS REAL)}.
   *
   * <p>SQL textual literal syntax has no marker for single-precision floats: the unparser writes a
   * FLOAT/REAL RexLiteral as the same bare numeric form as a DOUBLE one (e.g. {@code 6E-2}).
   * Calcite's parser then types every exponent-bearing numeric literal as DOUBLE, so the
   * round-tripped plan reports a DOUBLE column where the visitor produced FLOAT — contradicting
   * PPL's rule that {@code FLOAT - FLOAT} stays FLOAT. The CAST forces the unparser to emit
   * {@code CAST(6E-2 AS REAL)}, whose target type the parser reads explicitly. Both
   * {@link org.apache.calcite.sql.type.SqlTypeName#FLOAT} and
   * {@link org.apache.calcite.sql.type.SqlTypeName#REAL} are wrapped because Calcite's
   * {@code RexBuilder.makeCast} for {@code DECIMAL → REAL} folds the literal to a REAL-typed
   * RexLiteral, not a FLOAT-typed one. {@code makeAbstractCast} preserves the CAST as a Rex node
   * (a plain {@code makeCast} would constant-fold it back into a literal).
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
                .withDefaultNullCollation(NullCollation.LOW));

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

    SqlToRelConverter sqlToRel =
        new SqlToRelConverter(
            (rowType, queryString, schemaPath, viewPath) -> null,
            validator,
            catalogReader,
            context.relBuilder.getCluster(),
            new PplConvertletTable(),
            SqlToRelConverter.config().withTrimUnusedFields(false));

    RelRoot root = sqlToRel.convertQuery(validated, false, true);
    return root.project();
  }
}
