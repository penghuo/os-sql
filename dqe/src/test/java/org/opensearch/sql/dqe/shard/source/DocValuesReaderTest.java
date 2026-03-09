/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DocValuesReader: read Lucene doc values into Trino Blocks")
class DocValuesReaderTest {

  @Test
  @DisplayName("reads SortedNumericDocValues into LongArrayBlock (BIGINT)")
  void readsBigintDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      writeNumericDocs(dir, "age", 10L, 20L, 30L);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("age", BigintType.BIGINT);
        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
        int[] docIds = {0, 1, 2};

        DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

        Block block = builder.build();
        assertEquals(3, block.getPositionCount());
        assertEquals(10L, BigintType.BIGINT.getLong(block, 0));
        assertEquals(20L, BigintType.BIGINT.getLong(block, 1));
        assertEquals(30L, BigintType.BIGINT.getLong(block, 2));
      }
    }
  }

  @Test
  @DisplayName("reads double values stored as raw long bits")
  void readsDoubleDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (double val : new double[] {1.5, 2.7, 3.14}) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("price", Double.doubleToRawLongBits(val)));
          writer.addDocument(doc);
        }
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("price", DoubleType.DOUBLE);
        BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 3);
        int[] docIds = {0, 1, 2};

        DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

        Block block = builder.build();
        assertEquals(3, block.getPositionCount());
        assertEquals(1.5, DoubleType.DOUBLE.getDouble(block, 0), 0.001);
        assertEquals(2.7, DoubleType.DOUBLE.getDouble(block, 1), 0.001);
        assertEquals(3.14, DoubleType.DOUBLE.getDouble(block, 2), 0.001);
      }
    }
  }

  @Test
  @DisplayName("reads timestamp doc values (epoch millis → Trino micros)")
  void readsTimestampDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      // epoch millis: 2023-01-01 00:00:00 UTC = 1672531200000
      long epochMillis = 1672531200000L;
      writeNumericDocs(dir, "ts", epochMillis, epochMillis + 1000L);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("ts", TimestampType.TIMESTAMP_MILLIS);
        BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, 2);
        int[] docIds = {0, 1};

        DocValuesReader.readColumn(leaf, col, docIds, 2, builder);

        Block block = builder.build();
        assertEquals(2, block.getPositionCount());
        // Trino stores timestamps as micros
        assertEquals(epochMillis * 1000L, TimestampType.TIMESTAMP_MILLIS.getLong(block, 0));
        assertEquals(
            (epochMillis + 1000L) * 1000L, TimestampType.TIMESTAMP_MILLIS.getLong(block, 1));
      }
    }
  }

  @Test
  @DisplayName("reads boolean doc values (0=false, 1=true)")
  void readsBooleanDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      writeNumericDocs(dir, "flag", 1L, 0L, 1L);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("flag", BooleanType.BOOLEAN);
        BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 3);
        int[] docIds = {0, 1, 2};

        DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

        Block block = builder.build();
        assertEquals(3, block.getPositionCount());
        assertTrue(BooleanType.BOOLEAN.getBoolean(block, 0));
        assertTrue(!BooleanType.BOOLEAN.getBoolean(block, 1));
        assertTrue(BooleanType.BOOLEAN.getBoolean(block, 2));
      }
    }
  }

  @Test
  @DisplayName("null handling: missing doc values produce null in block")
  void nullHandlingForMissingDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        // Doc 0: has value
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField("val", 42L));
        writer.addDocument(doc0);
        // Doc 1: missing "val" field
        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField("other", 99L));
        writer.addDocument(doc1);
        // Doc 2: has value
        Document doc2 = new Document();
        doc2.add(new SortedNumericDocValuesField("val", 100L));
        writer.addDocument(doc2);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("val", BigintType.BIGINT);
        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
        int[] docIds = {0, 1, 2};

        DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

        Block block = builder.build();
        assertEquals(3, block.getPositionCount());
        assertEquals(42L, BigintType.BIGINT.getLong(block, 0));
        assertTrue(block.isNull(1));
        assertEquals(100L, BigintType.BIGINT.getLong(block, 2));
      }
    }
  }

  @Test
  @DisplayName("reads SortedSetDocValues into VariableWidthBlock (VARCHAR)")
  void readsKeywordDocValues() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (String val : new String[] {"hello", "world", "test"}) {
          Document doc = new Document();
          doc.add(new SortedSetDocValuesField("name", new BytesRef(val)));
          writer.addDocument(doc);
        }
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("name", VarcharType.VARCHAR);
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
        int[] docIds = {0, 1, 2};

        DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

        Block block = builder.build();
        assertEquals(3, block.getPositionCount());
        assertEquals("hello", VarcharType.VARCHAR.getSlice(block, 0).toStringUtf8());
        assertEquals("world", VarcharType.VARCHAR.getSlice(block, 1).toStringUtf8());
        assertEquals("test", VarcharType.VARCHAR.getSlice(block, 2).toStringUtf8());
      }
    }
  }

  @Test
  @DisplayName("keyword null handling: missing SortedSetDocValues produce null")
  void keywordNullHandling() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        // Doc 0: has keyword
        Document doc0 = new Document();
        doc0.add(new SortedSetDocValuesField("tag", new BytesRef("a")));
        writer.addDocument(doc0);
        // Doc 1: missing keyword field
        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField("other", 1L));
        writer.addDocument(doc1);
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext leaf = reader.leaves().get(0);
        ColumnHandle col = new ColumnHandle("tag", VarcharType.VARCHAR);
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
        int[] docIds = {0, 1};

        DocValuesReader.readColumn(leaf, col, docIds, 2, builder);

        Block block = builder.build();
        assertEquals(2, block.getPositionCount());
        assertEquals("a", VarcharType.VARCHAR.getSlice(block, 0).toStringUtf8());
        assertTrue(block.isNull(1));
      }
    }
  }

  /** Helper: writes documents with a single SortedNumericDocValuesField. */
  private static void writeNumericDocs(Directory dir, String field, long... values)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig();
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (long val : values) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField(field, val));
        writer.addDocument(doc);
      }
      writer.commit();
    }
  }
}
