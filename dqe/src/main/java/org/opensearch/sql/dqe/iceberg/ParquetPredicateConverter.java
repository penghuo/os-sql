/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.iceberg;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.StringLiteral;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

/**
 * Converts simple predicates from FilterNode into TupleDomain&lt;ColumnDescriptor&gt;
 * for Parquet row-group filtering. Handles: =, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=, BETWEEN, IN, AND.
 * Silently skips non-pushable predicates (LIKE, REGEXP, OR, NOT, complex expressions).
 */
public class ParquetPredicateConverter {

  private static final Logger log = LogManager.getLogger(ParquetPredicateConverter.class);
  private final Map<String, Type> columnTypeMap;

  public ParquetPredicateConverter(Map<String, Type> columnTypeMap) {
    this.columnTypeMap = columnTypeMap;
  }

  public TupleDomain<ColumnDescriptor> extractPredicates(
      DqePlanNode plan, MessageType fileSchema) {
    Map<String, Domain> columnDomains = new LinkedHashMap<>();

    plan.accept(new DqePlanVisitor<Void, Void>() {
      @Override
      public Void visitFilter(FilterNode node, Void context) {
        try {
          DqeSqlParser parser = new DqeSqlParser();
          Expression expr = parser.parseExpression(node.getPredicateString());
          extractFromExpression(expr, columnDomains);
        } catch (Exception e) {
          log.warn("Failed to parse predicate for pushdown: {}", node.getPredicateString(), e);
        }
        for (DqePlanNode child : node.getChildren()) {
          child.accept(this, context);
        }
        return null;
      }

      @Override
      public Void visitPlan(DqePlanNode node, Void context) {
        for (DqePlanNode child : node.getChildren()) {
          child.accept(this, context);
        }
        return null;
      }
    }, null);

    if (columnDomains.isEmpty()) {
      return TupleDomain.all();
    }

    Map<ColumnDescriptor, Domain> descriptorDomains = new HashMap<>();
    for (Map.Entry<String, Domain> entry : columnDomains.entrySet()) {
      ColumnDescriptor descriptor = findDescriptor(fileSchema, entry.getKey());
      if (descriptor != null) {
        descriptorDomains.put(descriptor, entry.getValue());
      }
    }

    return descriptorDomains.isEmpty()
        ? TupleDomain.all()
        : TupleDomain.withColumnDomains(descriptorDomains);
  }

  private void extractFromExpression(Expression expr, Map<String, Domain> domains) {
    if (expr instanceof LogicalExpression logical
        && logical.getOperator() == LogicalExpression.Operator.AND) {
      for (Expression term : logical.getTerms()) {
        extractFromExpression(term, domains);
      }
    } else if (expr instanceof ComparisonExpression cmp) {
      extractComparison(cmp, domains);
    } else if (expr instanceof BetweenPredicate between) {
      extractBetween(between, domains);
    } else if (expr instanceof InPredicate in) {
      extractIn(in, domains);
    }
    // LIKE, REGEXP, OR, NOT — not pushable, silently skip
  }

  private void extractComparison(ComparisonExpression cmp, Map<String, Domain> domains) {
    String column = extractColumnName(cmp.getLeft());
    Expression valueExpr = cmp.getRight();
    ComparisonExpression.Operator op = cmp.getOperator();

    if (column == null) {
      // Try reversed: literal on left, column on right
      column = extractColumnName(cmp.getRight());
      if (column == null) return;
      valueExpr = cmp.getLeft();
      op = flipOperator(op);
    }

    Type type = columnTypeMap.get(column);
    if (type == null) return;
    Object value = extractTypedValue(valueExpr, type);
    if (value == null) return;

    Domain domain = switch (op) {
      case EQUAL -> Domain.singleValue(type, value);
      case NOT_EQUAL -> Domain.create(
          ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), false);
      case GREATER_THAN -> Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false);
      case GREATER_THAN_OR_EQUAL -> Domain.create(
          ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false);
      case LESS_THAN -> Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false);
      case LESS_THAN_OR_EQUAL -> Domain.create(
          ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false);
      default -> null;
    };
    if (domain != null) {
      domains.merge(column, domain, Domain::intersect);
    }
  }

  private void extractBetween(BetweenPredicate between, Map<String, Domain> domains) {
    String column = extractColumnName(between.getValue());
    if (column == null) return;
    Type type = columnTypeMap.get(column);
    if (type == null) return;
    Object min = extractTypedValue(between.getMin(), type);
    Object max = extractTypedValue(between.getMax(), type);
    if (min == null || max == null) return;
    Domain domain = Domain.create(
        ValueSet.ofRanges(Range.range(type, min, true, max, true)), false);
    domains.merge(column, domain, Domain::intersect);
  }

  private void extractIn(InPredicate in, Map<String, Domain> domains) {
    String column = extractColumnName(in.getValue());
    if (column == null) return;
    Type type = columnTypeMap.get(column);
    if (type == null) return;
    if (!(in.getValueList() instanceof InListExpression list)) return;
    Domain combined = null;
    for (Expression valExpr : list.getValues()) {
      Object val = extractTypedValue(valExpr, type);
      if (val == null) return;
      Domain single = Domain.singleValue(type, val);
      combined = (combined == null) ? single : combined.union(single);
    }
    if (combined != null) {
      domains.merge(column, combined, Domain::intersect);
    }
  }

  private static String extractColumnName(Expression expr) {
    if (expr instanceof Identifier id) return id.getValue();
    return null;
  }

  private Object extractTypedValue(Expression expr, Type type) {
    if (expr instanceof LongLiteral lit) {
      long val = lit.getParsedValue();
      if (type instanceof DoubleType) return (double) val;
      return val;
    }
    if (expr instanceof DoubleLiteral lit) {
      return lit.getValue();
    }
    if (expr instanceof StringLiteral lit) {
      if (type instanceof VarcharType) {
        return io.airlift.slice.Slices.utf8Slice(lit.getValue());
      }
      return null;
    }
    if (expr instanceof GenericLiteral lit) {
      if (lit.getType().equalsIgnoreCase("DATE")) {
        if (type instanceof DateType) {
          return (long) LocalDate.parse(lit.getValue()).toEpochDay();
        }
        if (type instanceof TimestampType) {
          // DATE '2013-07-15' compared against TimestampType → epoch micros at midnight
          return LocalDate.parse(lit.getValue()).toEpochDay() * 86_400_000_000L;
        }
      }
      return null;
    }
    return null;
  }

  private static ComparisonExpression.Operator flipOperator(ComparisonExpression.Operator op) {
    return switch (op) {
      case GREATER_THAN -> ComparisonExpression.Operator.LESS_THAN;
      case GREATER_THAN_OR_EQUAL -> ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      case LESS_THAN -> ComparisonExpression.Operator.GREATER_THAN;
      case LESS_THAN_OR_EQUAL -> ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      default -> op;
    };
  }

  private static ColumnDescriptor findDescriptor(MessageType schema, String columnName) {
    for (ColumnDescriptor col : schema.getColumns()) {
      String[] path = col.getPath();
      if (path.length == 1 && path[0].equals(columnName)) {
        return col;
      }
    }
    return null;
  }
}
