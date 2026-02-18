/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * Tests for DocValues to Block type mapping (S2.3 from test plan). Validates that each DocValues
 * type is correctly converted to the corresponding Block type.
 */
class DocValuesToBlockTest {

  private Directory directory;
  private DirectoryReader reader;

  @BeforeEach
  void setUp() {
    directory = new ByteBuffersDirectory();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (reader != null) {
      reader.close();
    }
    directory.close();
  }

  @Test
  @DisplayName("NumericDocValues -> LongBlock: long values match exactly")
  void numericDocValuesToLongBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (long val : new long[] {100L, 200L, 300L}) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("amount", val));
        writer.addDocument(doc);
      }
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("amount", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1, 2}, 3);

    assertEquals(3, page.getPositionCount());
    LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
    assertEquals(100L, block.getLong(0));
    assertEquals(200L, block.getLong(1));
    assertEquals(300L, block.getLong(2));
  }

  @Test
  @DisplayName("NumericDocValues -> IntBlock: integer values preserved")
  void numericDocValuesToIntBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int val : new int[] {10, 20, 30}) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("age", val));
        writer.addDocument(doc);
      }
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsInt("age", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1, 2}, 3);

    assertEquals(3, page.getPositionCount());
    IntArrayBlock block = (IntArrayBlock) page.getBlock(0);
    assertEquals(10, block.getInt(0));
    assertEquals(20, block.getInt(1));
    assertEquals(30, block.getInt(2));
  }

  @Test
  @DisplayName("NumericDocValues -> ShortBlock: short values preserved")
  void numericDocValuesToShortBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("port", 8080));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsShort("port", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    ShortArrayBlock block = (ShortArrayBlock) page.getBlock(0);
    assertEquals((short) 8080, block.getShort(0));
  }

  @Test
  @DisplayName("NumericDocValues -> ByteBlock: byte values preserved")
  void numericDocValuesToByteBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("flags", 42));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsByte("flags", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    ByteArrayBlock block = (ByteArrayBlock) page.getBlock(0);
    assertEquals((byte) 42, block.getByte(0));
  }

  @Test
  @DisplayName("NumericDocValues -> DoubleBlock: double bits decoded correctly")
  void numericDocValuesToDoubleBlock() throws IOException {
    double[] values = {3.14, 2.71, -1.0};
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (double val : values) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("score", Double.doubleToLongBits(val)));
        writer.addDocument(doc);
      }
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsDouble("score", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1, 2}, 3);

    DoubleArrayBlock block = (DoubleArrayBlock) page.getBlock(0);
    assertEquals(3.14, block.getDouble(0), 1e-10);
    assertEquals(2.71, block.getDouble(1), 1e-10);
    assertEquals(-1.0, block.getDouble(2), 1e-10);
  }

  @Test
  @DisplayName("NumericDocValues -> BooleanBlock: 0/1 mapped correctly")
  void numericDocValuesToBooleanBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc1 = new Document();
      doc1.add(new NumericDocValuesField("active", 1));
      writer.addDocument(doc1);
      Document doc2 = new Document();
      doc2.add(new NumericDocValuesField("active", 0));
      writer.addDocument(doc2);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsBoolean("active", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1}, 2);

    BooleanArrayBlock block = (BooleanArrayBlock) page.getBlock(0);
    assertTrue(block.getBoolean(0));
    assertFalse(block.getBoolean(1));
  }

  @Test
  @DisplayName("SortedDocValues -> VariableWidthBlock: keyword values correct")
  void sortedDocValuesToBytesRefBlock() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (String val : new String[] {"alpha", "beta", "gamma"}) {
        Document doc = new Document();
        doc.add(new SortedDocValuesField("status", new BytesRef(val)));
        writer.addDocument(doc);
      }
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.sortedAsVarWidth("status", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1, 2}, 3);

    VariableWidthBlock block = (VariableWidthBlock) page.getBlock(0);
    assertEquals("alpha", block.getString(0));
    assertEquals("beta", block.getString(1));
    assertEquals("gamma", block.getString(2));
  }

  @Test
  @DisplayName("Null field handling: missing values produce null positions in Block")
  void nullFieldHandling() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Doc 0: has the field
      Document doc1 = new Document();
      doc1.add(new NumericDocValuesField("value", 42));
      writer.addDocument(doc1);
      // Doc 1: missing the field (will be null)
      Document doc2 = new Document();
      writer.addDocument(doc2);
      // Doc 2: has the field
      Document doc3 = new Document();
      doc3.add(new NumericDocValuesField("value", 99));
      writer.addDocument(doc3);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("value", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1, 2}, 3);

    Block block = page.getBlock(0);
    assertEquals(3, block.getPositionCount());
    assertFalse(block.isNull(0));
    assertTrue(block.isNull(1));
    assertFalse(block.isNull(2));
    assertEquals(42L, ((LongArrayBlock) block).getLong(0));
    assertEquals(99L, ((LongArrayBlock) block).getLong(2));
  }

  @Test
  @DisplayName("Date field -> LongBlock: epoch millis preserved")
  void dateFieldMappingToLongBlock() throws IOException {
    long epochMillis = 1700000000000L;
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("timestamp", epochMillis));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("timestamp", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
    assertEquals(epochMillis, block.getLong(0));
  }

  @Test
  @DisplayName("SortedNumericDocValues: first value of multi-valued field taken")
  void sortedNumericFirstValue() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new SortedNumericDocValuesField("tags", 10L));
      doc.add(new SortedNumericDocValuesField("tags", 20L));
      doc.add(new SortedNumericDocValuesField("tags", 30L));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns =
        List.of(
            new ColumnMapping(
                "tags",
                ColumnMapping.DocValuesType.SORTED_NUMERIC,
                ColumnMapping.BlockType.LONG,
                0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
    assertEquals(10L, block.getLong(0));
  }

  @Test
  @DisplayName("SortedSetDocValues: first value of multi-valued keyword field taken")
  void sortedSetFirstValue() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesField("colors", new BytesRef("blue")));
      doc.add(new SortedSetDocValuesField("colors", new BytesRef("red")));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns =
        List.of(
            new ColumnMapping(
                "colors",
                ColumnMapping.DocValuesType.SORTED_SET,
                ColumnMapping.BlockType.VARIABLE_WIDTH,
                0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    VariableWidthBlock block = (VariableWidthBlock) page.getBlock(0);
    // SortedSetDocValues returns ords in sorted order, so "blue" comes before "red"
    assertEquals("blue", block.getString(0));
  }

  @Test
  @DisplayName("BinaryDocValues -> VariableWidthBlock: IP address encoding")
  void binaryDocValuesToVarWidthBlock() throws IOException {
    byte[] ipBytes = new byte[] {(byte) 192, (byte) 168, 1, 1};
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new BinaryDocValuesField("ip", new BytesRef(ipBytes)));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.binaryAsVarWidth("ip", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0}, 1);

    VariableWidthBlock block = (VariableWidthBlock) page.getBlock(0);
    byte[] result = block.getSlice(0);
    assertEquals(4, result.length);
    assertEquals((byte) 192, result[0]);
    assertEquals((byte) 168, result[1]);
    assertEquals(1, result[2]);
    assertEquals(1, result[3]);
  }

  @Test
  @DisplayName("Multiple columns in a single Page")
  void multipleColumnsInPage() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 1L));
      doc.add(new SortedDocValuesField("name", new BytesRef("Alice")));
      doc.add(new NumericDocValuesField("age", 30));
      writer.addDocument(doc);

      Document doc2 = new Document();
      doc2.add(new NumericDocValuesField("id", 2L));
      doc2.add(new SortedDocValuesField("name", new BytesRef("Bob")));
      doc2.add(new NumericDocValuesField("age", 25));
      writer.addDocument(doc2);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns =
        List.of(
            ColumnMapping.numericAsLong("id", 0),
            ColumnMapping.sortedAsVarWidth("name", 1),
            ColumnMapping.numericAsInt("age", 2));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[] {0, 1}, 2);

    assertEquals(2, page.getPositionCount());
    assertEquals(3, page.getChannelCount());

    LongArrayBlock idBlock = (LongArrayBlock) page.getBlock(0);
    assertEquals(1L, idBlock.getLong(0));
    assertEquals(2L, idBlock.getLong(1));

    VariableWidthBlock nameBlock = (VariableWidthBlock) page.getBlock(1);
    assertEquals("Alice", nameBlock.getString(0));
    assertEquals("Bob", nameBlock.getString(1));

    IntArrayBlock ageBlock = (IntArrayBlock) page.getBlock(2);
    assertEquals(30, ageBlock.getInt(0));
    assertEquals(25, ageBlock.getInt(1));
  }

  @Test
  @DisplayName("Empty doc list produces empty Page")
  void emptyDocList() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("x", 1L));
      writer.addDocument(doc);
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("x", 0));

    Page page = DocValuesToBlockConverter.readDocValues(leaf, columns, new int[0], 0);

    assertEquals(0, page.getPositionCount());
  }

  @Test
  @DisplayName("readAllDocs reads entire segment")
  void readAllDocsFromSegment() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < 5; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("n", i * 10L));
        writer.addDocument(doc);
      }
    }

    reader = DirectoryReader.open(directory);
    LeafReaderContext leaf = reader.leaves().get(0);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("n", 0));

    Page page = DocValuesToBlockConverter.readAllDocs(leaf, columns);

    assertEquals(5, page.getPositionCount());
    LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
    for (int i = 0; i < 5; i++) {
      assertEquals(i * 10L, block.getLong(i));
    }
  }
}
