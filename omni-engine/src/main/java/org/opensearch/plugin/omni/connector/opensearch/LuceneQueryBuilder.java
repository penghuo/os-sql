/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts Trino TupleDomain predicates to Lucene queries.
 * Uses Lucene point queries for numeric types and term queries for keywords.
 */
public final class LuceneQueryBuilder
{
    private LuceneQueryBuilder() {}

    public static Query build(TupleDomain<OpenSearchColumnHandle> predicate, Map<String, String> likePatterns,
            Optional<String> queryStringExpression)
    {
        BooleanQuery.Builder bool = new BooleanQuery.Builder();
        boolean hasClauses = false;

        // Domain predicates
        if (!predicate.isNone() && predicate.getDomains().isPresent()) {
            for (Map.Entry<OpenSearchColumnHandle, Domain> entry : predicate.getDomains().get().entrySet()) {
                OpenSearchColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();
                if (!column.isSupportsPredicates()) {
                    continue;
                }
                Query clause = buildDomainQuery(column.getOpensearchName(), column.getType(), domain);
                if (clause != null) {
                    bool.add(clause, BooleanClause.Occur.FILTER);
                    hasClauses = true;
                }
            }
        }

        // LIKE patterns → Lucene WildcardQuery
        for (Map.Entry<String, String> entry : likePatterns.entrySet()) {
            String fieldName = entry.getKey();
            String pattern = entry.getValue();
            Query wildcardQuery = buildWildcardQuery(fieldName, pattern);
            if (wildcardQuery != null) {
                bool.add(wildcardQuery, BooleanClause.Occur.FILTER);
                hasClauses = true;
            }
        }

        // query_string → Lucene QueryStringQueryParser
        if (queryStringExpression.isPresent()) {
            Query qsQuery = buildQueryStringQuery(queryStringExpression.get());
            if (qsQuery != null) {
                bool.add(qsQuery, BooleanClause.Occur.FILTER);
                hasClauses = true;
            }
        }

        if (predicate.isNone()) {
            return new MatchNoDocsQuery();
        }

        return hasClauses ? bool.build() : new MatchAllDocsQuery();
    }

    /**
     * Convert SQL LIKE pattern to Lucene WildcardQuery.
     * SQL LIKE: % = any chars, _ = single char
     * Lucene:   * = any chars, ? = single char
     */
    private static Query buildWildcardQuery(String fieldName, String sqlPattern)
    {
        // Convert SQL LIKE wildcards to Lucene wildcards
        StringBuilder lucenePattern = new StringBuilder();
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            switch (c) {
                case '%' -> lucenePattern.append('*');
                case '_' -> lucenePattern.append('?');
                case '*', '?' -> {
                    // Escape Lucene special chars
                    lucenePattern.append('\\').append(c);
                }
                default -> lucenePattern.append(c);
            }
        }
        return new WildcardQuery(new Term(fieldName, lucenePattern.toString()));
    }

    /**
     * Parse a query_string expression using Lucene's QueryParser.
     * Supports Lucene query syntax: field:value, field:value AND field:value, wildcards, etc.
     */
    private static Query buildQueryStringQuery(String queryString)
    {
        try {
            org.apache.lucene.queryparser.classic.QueryParser parser =
                    new org.apache.lucene.queryparser.classic.QueryParser(
                            "_all", new org.apache.lucene.analysis.standard.StandardAnalyzer());
            parser.setAllowLeadingWildcard(true);
            return parser.parse(queryString);
        }
        catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new RuntimeException("Failed to parse query_string: " + queryString, e);
        }
    }

    private static Query buildDomainQuery(String fieldName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return null; // No filter needed
        }
        if (domain.getValues().isNone()) {
            // IS NULL
            return new BooleanQuery.Builder()
                    .add(new FieldExistsQuery(fieldName), BooleanClause.Occur.MUST_NOT)
                    .build();
        }
        if (domain.getValues().isAll()) {
            // IS NOT NULL
            return new FieldExistsQuery(fieldName);
        }

        ValueSet valueSet = domain.getValues();
        List<Range> ranges = valueSet.getRanges().getOrderedRanges();

        if (ranges.size() == 1 && ranges.get(0).isSingleValue()) {
            // Equality: field = value
            Query eq = buildEqualityQuery(fieldName, type, ranges.get(0).getSingleValue());
            if (domain.isNullAllowed()) {
                return orNull(fieldName, eq);
            }
            return eq;
        }

        // Multiple ranges or non-equality
        BooleanQuery.Builder orBuilder = new BooleanQuery.Builder();
        for (Range range : ranges) {
            Query rangeQuery;
            if (range.isSingleValue()) {
                rangeQuery = buildEqualityQuery(fieldName, type, range.getSingleValue());
            } else {
                rangeQuery = buildRangeQuery(fieldName, type, range);
            }
            if (rangeQuery != null) {
                orBuilder.add(rangeQuery, BooleanClause.Occur.SHOULD);
            }
        }
        if (domain.isNullAllowed()) {
            orBuilder.add(
                    new BooleanQuery.Builder()
                            .add(new FieldExistsQuery(fieldName), BooleanClause.Occur.MUST_NOT)
                            .build(),
                    BooleanClause.Occur.SHOULD);
        }
        return orBuilder.build();
    }

    private static Query buildEqualityQuery(String fieldName, Type type, Object value)
    {
        if (type.equals(VarcharType.VARCHAR)) {
            return new TermQuery(new Term(fieldName, ((io.airlift.slice.Slice) value).toStringUtf8()));
        }
        if (type.equals(BigintType.BIGINT) || type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            long longVal = (long) value;
            if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
                longVal = longVal / 1000; // Trino micros -> millis
            }
            return LongPoint.newExactQuery(fieldName, longVal);
        }
        if (type.equals(IntegerType.INTEGER)) {
            return IntPoint.newExactQuery(fieldName, (int) (long) value);
        }
        if (type.equals(SmallintType.SMALLINT) || type.equals(TinyintType.TINYINT)) {
            // OpenSearch indexes short/byte as IntPoint (half_float uses HalfFloatPoint)
            return IntPoint.newExactQuery(fieldName, (int) (long) value);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return DoublePoint.newExactQuery(fieldName, (double) value);
        }
        if (type.equals(RealType.REAL)) {
            // Trino stores REAL as int bits in a long; convert to float for HalfFloatPoint
            float floatVal = Float.intBitsToFloat((int) (long) value);
            return DoublePoint.newExactQuery(fieldName, floatVal);
        }
        if (type.equals(BooleanType.BOOLEAN)) {
            // OpenSearch indexes booleans as "T"/"F" in keyword field
            return new TermQuery(new Term(fieldName, ((boolean) value) ? "T" : "F"));
        }
        return null; // Unsupported type for pushdown
    }

    private static Query buildRangeQuery(String fieldName, Type type, Range range)
    {
        if (type.equals(BigintType.BIGINT) || type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            long low = range.isLowUnbounded() ? Long.MIN_VALUE : (long) range.getLowBoundedValue();
            long high = range.isHighUnbounded() ? Long.MAX_VALUE : (long) range.getHighBoundedValue();
            if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
                low = range.isLowUnbounded() ? low : low / 1000;
                high = range.isHighUnbounded() ? high : high / 1000;
            }
            if (!range.isLowInclusive() && !range.isLowUnbounded()) low = Math.addExact(low, 1);
            if (!range.isHighInclusive() && !range.isHighUnbounded()) high = Math.addExact(high, -1);
            return LongPoint.newRangeQuery(fieldName, low, high);
        }
        if (type.equals(IntegerType.INTEGER) || type.equals(SmallintType.SMALLINT) || type.equals(TinyintType.TINYINT)) {
            int low = range.isLowUnbounded() ? Integer.MIN_VALUE : (int) (long) range.getLowBoundedValue();
            int high = range.isHighUnbounded() ? Integer.MAX_VALUE : (int) (long) range.getHighBoundedValue();
            if (!range.isLowInclusive() && !range.isLowUnbounded()) low++;
            if (!range.isHighInclusive() && !range.isHighUnbounded()) high--;
            return IntPoint.newRangeQuery(fieldName, low, high);
        }
        if (type.equals(DoubleType.DOUBLE) || type.equals(RealType.REAL)) {
            double low = range.isLowUnbounded() ? Double.NEGATIVE_INFINITY : (double) range.getLowBoundedValue();
            double high = range.isHighUnbounded() ? Double.POSITIVE_INFINITY : (double) range.getHighBoundedValue();
            if (!range.isLowInclusive() && !range.isLowUnbounded()) low = Math.nextUp(low);
            if (!range.isHighInclusive() && !range.isHighUnbounded()) high = Math.nextDown(high);
            return DoublePoint.newRangeQuery(fieldName, low, high);
        }
        if (type.equals(VarcharType.VARCHAR)) {
            // Term range for keyword fields
            String low = range.isLowUnbounded() ? null : ((io.airlift.slice.Slice) range.getLowBoundedValue()).toStringUtf8();
            String high = range.isHighUnbounded() ? null : ((io.airlift.slice.Slice) range.getHighBoundedValue()).toStringUtf8();
            return TermRangeQuery.newStringRange(fieldName, low, high, range.isLowInclusive(), range.isHighInclusive());
        }
        return null;
    }

    private static Query orNull(String fieldName, Query query)
    {
        return new BooleanQuery.Builder()
                .add(query, BooleanClause.Occur.SHOULD)
                .add(new BooleanQuery.Builder()
                        .add(new FieldExistsQuery(fieldName), BooleanClause.Occur.MUST_NOT)
                        .build(), BooleanClause.Occur.SHOULD)
                .build();
    }
}
