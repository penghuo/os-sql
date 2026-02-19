/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.sql.distributed.operator.SortOrder;

/**
 * Converts distributed engine sort specifications (channel + SortOrder) into Lucene {@link Sort}
 * objects for use with {@link org.apache.lucene.search.IndexSearcher#search(
 * org.apache.lucene.search.Query, int, Sort)}.
 *
 * <p>Handles mapping from column metadata to the appropriate Lucene SortField type:
 *
 * <ul>
 *   <li>Numeric fields (LONG, INT, DOUBLE, FLOAT) → {@link SortedNumericSortField}
 *   <li>Keyword fields (SORTED, SORTED_SET) → {@link SortedSetSortField} or {@link SortField}
 * </ul>
 */
public final class LuceneSortFieldConverter {

  private LuceneSortFieldConverter() {}

  /**
   * Converts a list of sort specifications into a Lucene {@link Sort}.
   *
   * @param sortColumns column mappings for the sort fields
   * @param sortOrders corresponding sort orders (ASC/DESC, nulls first/last)
   * @return the Lucene Sort object
   */
  public static Sort convert(List<ColumnMapping> sortColumns, List<SortOrder> sortOrders) {
    if (sortColumns.size() != sortOrders.size()) {
      throw new IllegalArgumentException("sortColumns and sortOrders must have the same size");
    }

    List<SortField> sortFields = new ArrayList<>(sortColumns.size());
    for (int i = 0; i < sortColumns.size(); i++) {
      sortFields.add(convertOne(sortColumns.get(i), sortOrders.get(i)));
    }
    return new Sort(sortFields.toArray(new SortField[0]));
  }

  /** Converts a single column + sort order into a Lucene SortField. */
  static SortField convertOne(ColumnMapping column, SortOrder sortOrder) {
    boolean reverse = !sortOrder.isAscending();

    return switch (column.getDocValuesType()) {
      case NUMERIC -> {
        SortField.Type sfType = numericSortFieldType(column.getBlockType());
        SortedNumericSortField sf =
            new SortedNumericSortField(column.getFieldName(), sfType, reverse);
        configureMissingValue(sf, sortOrder, column.getBlockType());
        yield sf;
      }
      case SORTED_NUMERIC -> {
        SortField.Type sfType = numericSortFieldType(column.getBlockType());
        SortedNumericSortField sf =
            new SortedNumericSortField(column.getFieldName(), sfType, reverse);
        configureMissingValue(sf, sortOrder, column.getBlockType());
        yield sf;
      }
      case SORTED -> {
        SortField sf = new SortField(column.getFieldName(), SortField.Type.STRING, reverse);
        if (sortOrder.isNullsFirst()) {
          sf.setMissingValue(reverse ? SortField.STRING_LAST : SortField.STRING_FIRST);
        } else {
          sf.setMissingValue(reverse ? SortField.STRING_FIRST : SortField.STRING_LAST);
        }
        yield sf;
      }
      case SORTED_SET -> {
        SortedSetSortField sf = new SortedSetSortField(column.getFieldName(), reverse);
        if (sortOrder.isNullsFirst()) {
          sf.setMissingValue(reverse ? SortField.STRING_LAST : SortField.STRING_FIRST);
        } else {
          sf.setMissingValue(reverse ? SortField.STRING_FIRST : SortField.STRING_LAST);
        }
        yield sf;
      }
      default ->
          throw new UnsupportedOperationException(
              "Cannot create SortField for DocValues type: " + column.getDocValuesType());
    };
  }

  /** Maps a BlockType to the corresponding Lucene SortField.Type for numeric sort. */
  private static SortField.Type numericSortFieldType(ColumnMapping.BlockType blockType) {
    return switch (blockType) {
      case LONG -> SortField.Type.LONG;
      case INT, SHORT, BYTE -> SortField.Type.INT;
      case DOUBLE -> SortField.Type.DOUBLE;
      case FLOAT -> SortField.Type.FLOAT;
      default -> SortField.Type.LONG;
    };
  }

  /** Configures the missing value for a numeric SortField based on null ordering. */
  private static void configureMissingValue(
      SortField sf, SortOrder sortOrder, ColumnMapping.BlockType blockType) {
    if (sortOrder.isNullsFirst()) {
      // Nulls first: use the smallest possible value so nulls sort before real values
      // (for ASC), or the largest value (for DESC via reverse)
      sf.setMissingValue(
          sortOrder.isAscending() ? numericMinValue(blockType) : numericMaxValue(blockType));
    } else {
      // Nulls last: use the largest possible value so nulls sort after real values
      // (for ASC), or the smallest value (for DESC via reverse)
      sf.setMissingValue(
          sortOrder.isAscending() ? numericMaxValue(blockType) : numericMinValue(blockType));
    }
  }

  private static Object numericMinValue(ColumnMapping.BlockType blockType) {
    return switch (blockType) {
      case LONG -> Long.MIN_VALUE;
      case INT, SHORT, BYTE -> Integer.MIN_VALUE;
      case DOUBLE -> Double.NEGATIVE_INFINITY;
      case FLOAT -> Float.NEGATIVE_INFINITY;
      default -> Long.MIN_VALUE;
    };
  }

  private static Object numericMaxValue(ColumnMapping.BlockType blockType) {
    return switch (blockType) {
      case LONG -> Long.MAX_VALUE;
      case INT, SHORT, BYTE -> Integer.MAX_VALUE;
      case DOUBLE -> Double.POSITIVE_INFINITY;
      case FLOAT -> Float.POSITIVE_INFINITY;
      default -> Long.MAX_VALUE;
    };
  }
}
