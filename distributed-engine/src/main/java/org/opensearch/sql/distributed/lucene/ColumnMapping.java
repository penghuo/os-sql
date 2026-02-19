/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import java.util.Objects;

/**
 * Describes how a single projected column maps from an OpenSearch field to a Lucene DocValues type
 * and the corresponding Block type used by the distributed engine.
 *
 * <p>This is the central type-mapping abstraction for the Lucene leaf operators. It tells {@link
 * DocValuesToBlockConverter} which DocValues API to use for each field and how to write the values
 * into a BlockBuilder.
 */
public final class ColumnMapping {

  /** The types of DocValues storage used by Lucene/OpenSearch. */
  public enum DocValuesType {
    /** Long-encoded numerics: long, int, short, byte, boolean, date (epoch millis). */
    NUMERIC,
    /** Sorted byte[] for keyword fields (single-valued). */
    SORTED,
    /** Multi-valued numerics (e.g., array of longs). */
    SORTED_NUMERIC,
    /** Multi-valued sorted byte[] (e.g., array of keywords). */
    SORTED_SET,
    /** Raw binary data. */
    BINARY,
    /** Field has no DocValues; produces all-null output blocks. */
    NONE
  }

  /** The Block type that the DocValues should be converted into. */
  public enum BlockType {
    LONG,
    INT,
    SHORT,
    BYTE,
    DOUBLE,
    FLOAT,
    BOOLEAN,
    VARIABLE_WIDTH
  }

  private final String fieldName;
  private final DocValuesType docValuesType;
  private final BlockType blockType;
  private final int columnIndex;
  private final boolean ipField;

  public ColumnMapping(
      String fieldName, DocValuesType docValuesType, BlockType blockType, int columnIndex) {
    this(fieldName, docValuesType, blockType, columnIndex, false);
  }

  public ColumnMapping(
      String fieldName,
      DocValuesType docValuesType,
      BlockType blockType,
      int columnIndex,
      boolean ipField) {
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    this.docValuesType = Objects.requireNonNull(docValuesType, "docValuesType");
    this.blockType = Objects.requireNonNull(blockType, "blockType");
    this.columnIndex = columnIndex;
    this.ipField = ipField;
  }

  public String getFieldName() {
    return fieldName;
  }

  public DocValuesType getDocValuesType() {
    return docValuesType;
  }

  public BlockType getBlockType() {
    return blockType;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  /** Returns true if this column is an IP address field requiring binary-to-string decoding. */
  public boolean isIpField() {
    return ipField;
  }

  /**
   * Creates a ColumnMapping for a numeric field that stores values as longs in DocValues (long,
   * integer, short, byte, date, boolean).
   */
  public static ColumnMapping numericAsLong(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.LONG, columnIndex);
  }

  /** Creates a ColumnMapping for an integer field read from NumericDocValues. */
  public static ColumnMapping numericAsInt(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.INT, columnIndex);
  }

  /** Creates a ColumnMapping for a short field read from NumericDocValues. */
  public static ColumnMapping numericAsShort(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.SHORT, columnIndex);
  }

  /** Creates a ColumnMapping for a byte field read from NumericDocValues. */
  public static ColumnMapping numericAsByte(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.BYTE, columnIndex);
  }

  /** Creates a ColumnMapping for a double field stored as long bits in NumericDocValues. */
  public static ColumnMapping numericAsDouble(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.DOUBLE, columnIndex);
  }

  /** Creates a ColumnMapping for a float field stored as int bits in NumericDocValues. */
  public static ColumnMapping numericAsFloat(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.FLOAT, columnIndex);
  }

  /** Creates a ColumnMapping for a boolean field stored as 0/1 in NumericDocValues. */
  public static ColumnMapping numericAsBoolean(String fieldName, int columnIndex) {
    return new ColumnMapping(fieldName, DocValuesType.NUMERIC, BlockType.BOOLEAN, columnIndex);
  }

  /** Creates a ColumnMapping for a keyword field using SortedDocValues. */
  public static ColumnMapping sortedAsVarWidth(String fieldName, int columnIndex) {
    return new ColumnMapping(
        fieldName, DocValuesType.SORTED, BlockType.VARIABLE_WIDTH, columnIndex);
  }

  /** Creates a ColumnMapping for a binary field using BinaryDocValues. */
  public static ColumnMapping binaryAsVarWidth(String fieldName, int columnIndex) {
    return new ColumnMapping(
        fieldName, DocValuesType.BINARY, BlockType.VARIABLE_WIDTH, columnIndex);
  }

  @Override
  public String toString() {
    return "ColumnMapping{"
        + "field="
        + fieldName
        + ", dv="
        + docValuesType
        + ", block="
        + blockType
        + ", col="
        + columnIndex
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ColumnMapping)) return false;
    ColumnMapping that = (ColumnMapping) o;
    return columnIndex == that.columnIndex
        && ipField == that.ipField
        && fieldName.equals(that.fieldName)
        && docValuesType == that.docValuesType
        && blockType == that.blockType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, docValuesType, blockType, columnIndex, ipField);
  }
}
