/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.TransactionId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.trino.transaction.TransactionManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.opensearch.plugin.omni.ppl.OmniSqlDialect;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler;
import org.opensearch.sql.executor.QueryType;

import static io.airlift.concurrent.MoreFutures.getFutureValue;

/**
 * Translates PPL queries to Trino SQL using os-sql's UnifiedQueryContext.
 */
public class PplTranslator {
    private static final Logger log = LogManager.getLogger(PplTranslator.class);

    private final Metadata metadata;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    public PplTranslator(Metadata metadata, TransactionManager transactionManager, AccessControl accessControl) {
        this.metadata = metadata;
        this.transactionManager = transactionManager;
        this.accessControl = accessControl;
    }

    /**
     * Translates a PPL query string to Trino SQL.
     *
     * @param ppl the PPL query (e.g., "source=hits | stats count()")
     * @param session the Trino session (provides catalog/schema context)
     * @return the equivalent Trino SQL string
     */
    public String translate(String ppl, Session session) {
        String catalogName = session.getCatalog()
                .orElseThrow(() -> new IllegalStateException("No catalog set in session"));
        String schemaName = session.getSchema().orElse("default");

        // Begin a transaction for metadata access
        TransactionId transactionId = transactionManager.beginTransaction(true);
        Session txnSession = session.beginTransactionId(transactionId, transactionManager, accessControl);

        try {
            metadata.beginQuery(txnSession);

            Schema calciteSchema = new CalciteSchemaAdapter(metadata, txnSession, catalogName);

            try (UnifiedQueryContext ctx = UnifiedQueryContext.builder()
                    .language(QueryType.PPL)
                    .catalog(catalogName, calciteSchema)
                    .defaultNamespace(catalogName + "." + schemaName)
                    .setting("plugins.calcite.enabled", true)
                    .setting("plugins.ppl.rex.max_match.limit", 10)
                    .build()) {
                RelNode plan = new UnifiedQueryPlanner(ctx).plan(ppl);
                String sql = UnifiedQueryTranspiler.builder()
                        .dialect(OmniSqlDialect.DEFAULT)
                        .build()
                        .toSql(plan);

                getFutureValue(transactionManager.asyncCommit(transactionId));
                // Dotted column names: "cloud.region" → "cloud"."region"
                // (identifier unparsing is in SqlIdentifier, not dialect)
                sql = rewriteDottedColumns(sql);
                // ILIKE → LOWER(x) LIKE LOWER(y) — os-sql custom operator, not routed through dialect
                sql = rewriteIlike(sql);
                // QUERY_STRING(MAP(...)) → "query_string"('value') — os-sql custom operator
                sql = rewriteQueryString(sql);
                // COALESCE mixed types: cast all args to VARCHAR when mixing types
                // (dialect doesn't have type info at unparse time)
                sql = rewriteCoalesce(sql);
                // REGEXP(col, pattern) → regexp_like(col, pattern) — os-sql REGEXP operator → Trino function
                sql = rewriteRegexp(sql);
                return sql;
            }
        } catch (Exception e) {
            transactionManager.asyncAbort(transactionId);
            throw new RuntimeException("Failed to translate PPL to SQL: " + e.getMessage(), e);
        }
    }

    /**
     * Rewrites ILIKE to LOWER(x) LIKE LOWER(pattern) since Trino doesn't support ILIKE.
     * os-sql emits ILIKE as a custom operator not routed through dialect.unparseCall().
     */
    static String rewriteIlike(String sql) {
        return sql.replaceAll(
                "(\\S+)\\s+ILIKE\\s+(\\S+)",
                "LOWER($1) LIKE LOWER($2)");
    }

    /**
     * Rewrites QUERY_STRING(MAP(ARRAY[...], ARRAY[...])) → "query_string"('value').
     * os-sql emits QUERY_STRING as a custom operator not routed through dialect.unparseCall().
     */
    static String rewriteQueryString(String sql) {
        sql = java.util.regex.Pattern.compile(
                "QUERY_STRING\\(MAP \\(ARRAY\\['query'\\], ARRAY\\['([^']*)'\\]\\)\\)")
                .matcher(sql)
                .replaceAll("\"query_string\"('$1')");
        sql = java.util.regex.Pattern.compile(
                "QUERY_STRING\\(.*?,\\s*MAP \\(ARRAY\\['query'\\], ARRAY\\['([^']*)'\\]\\)\\)")
                .matcher(sql)
                .replaceAll("\"query_string\"('$1')");
        return sql;
    }

    /**
     * Rewrites quoted dotted column names to struct field access for Trino.
     * "cloud.region" → "cloud"."region"
     * Preserves aliases: AS "cloud.region" stays as-is.
     */
    static String rewriteDottedColumns(String sql) {
        return java.util.regex.Pattern.compile("(?<!AS )\"([a-zA-Z_@][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\"")
                .matcher(sql)
                .replaceAll(match -> {
                    String name = match.group(1);
                    if (!name.contains(".")) {
                        return java.util.regex.Matcher.quoteReplacement(match.group());
                    }
                    String[] parts = name.split("\\.");
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < parts.length; i++) {
                        if (i > 0) sb.append(".");
                        sb.append("\"").append(parts[i]).append("\"");
                    }
                    return java.util.regex.Matcher.quoteReplacement(sb.toString());
                });
    }

    /**
     * Rewrites COALESCE args to cast all to VARCHAR when a string literal is present.
     * COALESCE(NULL, "host"."name", "metrics"."size", 'unknown')
     * → COALESCE(NULL, CAST("host"."name" AS VARCHAR), CAST("metrics"."size" AS VARCHAR), 'unknown')
     * This stays as a string rewrite because the dialect doesn't have type info at unparse time.
     */
    static String rewriteCoalesce(String sql) {
        return java.util.regex.Pattern.compile("COALESCE\\(([^)]+)\\)")
                .matcher(sql)
                .replaceAll(match -> {
                    String argsStr = match.group(1);
                    String[] args = argsStr.split(",\\s*");
                    // Check if there's a mix: at least one string literal and at least one column ref
                    boolean hasStringLiteral = false;
                    boolean hasColumnRef = false;
                    for (String arg : args) {
                        String trimmed = arg.trim();
                        if (trimmed.startsWith("'") && trimmed.endsWith("'")) hasStringLiteral = true;
                        else if (trimmed.startsWith("\"")) hasColumnRef = true;
                    }
                    if (!hasStringLiteral || !hasColumnRef) {
                        return java.util.regex.Matcher.quoteReplacement(match.group());
                    }
                    // Cast all non-NULL, non-string-literal args to VARCHAR
                    StringBuilder sb = new StringBuilder("COALESCE(");
                    for (int i = 0; i < args.length; i++) {
                        if (i > 0) sb.append(", ");
                        String trimmed = args[i].trim();
                        if (trimmed.equals("NULL") || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
                            sb.append(trimmed);
                        } else {
                            sb.append("CAST(").append(trimmed).append(" AS VARCHAR)");
                        }
                    }
                    sb.append(")");
                    return java.util.regex.Matcher.quoteReplacement(sb.toString());
                });
    }

    /**
     * Rewrites REGEXP to regexp_like since Trino doesn't have a REGEXP function.
     * os-sql emits REGEXP(col, pattern) but Trino uses regexp_like(col, pattern).
     */
    static String rewriteRegexp(String sql) {
        return sql.replaceAll("\\bREGEXP\\s*\\(", "regexp_like(");
    }

}
