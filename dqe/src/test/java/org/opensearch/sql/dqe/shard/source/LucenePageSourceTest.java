/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LucenePageSource: segment-streaming doc values page source")
class LucenePageSourceTest {

  @Test
  @DisplayName("scans all documents with MatchAllDocsQuery in batches")
  void scansAllDocsInBatches() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("id", i));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<ColumnHandle> columns = List.of(new ColumnHandle("id", BigintType.BIGINT));

        LucenePageSource source = new LucenePageSource(reader, new MatchAllDocsQuery(), columns, 3);

        // Should return 2 pages: [0,1,2] and [3,4]
        Page p1 = source.processNextBatch();
        assertNotNull(p1);
        assertEquals(3, p1.getPositionCount());
        assertEquals(1, p1.getChannelCount());
        assertEquals(0L, BigintType.BIGINT.getLong(p1.getBlock(0), 0));
        assertEquals(1L, BigintType.BIGINT.getLong(p1.getBlock(0), 1));
        assertEquals(2L, BigintType.BIGINT.getLong(p1.getBlock(0), 2));

        Page p2 = source.processNextBatch();
        assertNotNull(p2);
        assertEquals(2, p2.getPositionCount());
        assertEquals(3L, BigintType.BIGINT.getLong(p2.getBlock(0), 0));
        assertEquals(4L, BigintType.BIGINT.getLong(p2.getBlock(0), 1));

        assertNull(source.processNextBatch()); // exhausted

        source.close();
      }
    }
  }

  @Test
  @DisplayName("COUNT(*) fast path: no columns, returns position count only")
  void countStarFastPath() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 100; i++) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("x", i));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LucenePageSource source =
            new LucenePageSource(reader, new MatchAllDocsQuery(), List.of(), 1000);

        Page page = source.processNextBatch();
        assertNotNull(page);
        assertEquals(100, page.getPositionCount());
        assertEquals(0, page.getChannelCount());

        assertNull(source.processNextBatch()); // exhausted
        source.close();
      }
    }
  }

  @Test
  @DisplayName("multi-column scan with mixed types")
  void multiColumnScan() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (int i = 0; i < 3; i++) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("id", i));
          doc.add(new SortedSetDocValuesField("name", new BytesRef("item" + i)));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<ColumnHandle> columns =
            List.of(
                new ColumnHandle("id", BigintType.BIGINT),
                new ColumnHandle("name", VarcharType.VARCHAR));

        LucenePageSource source =
            new LucenePageSource(reader, new MatchAllDocsQuery(), columns, 10);

        Page page = source.processNextBatch();
        assertNotNull(page);
        assertEquals(3, page.getPositionCount());
        assertEquals(2, page.getChannelCount());

        assertEquals(0L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
        assertEquals("item0", VarcharType.VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8());
        assertEquals("item2", VarcharType.VARCHAR.getSlice(page.getBlock(1), 2).toStringUtf8());

        assertNull(source.processNextBatch());
        source.close();
      }
    }
  }

  @Test
  @DisplayName("empty index returns null immediately")
  void emptyIndex() throws Exception {
    try (Directory dir = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<ColumnHandle> columns = List.of(new ColumnHandle("id", BigintType.BIGINT));
        LucenePageSource source =
            new LucenePageSource(reader, new MatchAllDocsQuery(), columns, 10);

        assertNull(source.processNextBatch());
        source.close();
      }
    }
  }
}
