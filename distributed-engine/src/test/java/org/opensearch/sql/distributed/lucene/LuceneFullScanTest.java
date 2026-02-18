/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/** Tests for LuceneFullScan operator (S2.2 from test plan). */
class LuceneFullScanTest {

  private Directory directory;
  private DirectoryReader reader;
  private IndexSearcher searcher;

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

  private OperatorContext createContext() {
    return new OperatorContext(0, "LuceneFullScan");
  }

  private void writeDocuments(int count) throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < count; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", i));
        doc.add(new SortedDocValuesField("name", new BytesRef("doc" + i)));
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @Test
  @DisplayName("Scan all documents from single segment")
  void scanAllDocsSingleSegment() throws Exception {
    writeDocuments(5);
    List<ColumnMapping> columns =
        List.of(ColumnMapping.numericAsLong("id", 0), ColumnMapping.sortedAsVarWidth("name", 1));

    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    // Verify values in order
    Page firstPage = pages.get(0);
    LongArrayBlock idBlock = (LongArrayBlock) firstPage.getBlock(0);
    assertEquals(0L, idBlock.getLong(0));

    VariableWidthBlock nameBlock = (VariableWidthBlock) firstPage.getBlock(1);
    assertEquals("doc0", nameBlock.getString(0));

    scan.close();
  }

  @Test
  @DisplayName("Scan with multiple segments")
  void scanMultipleSegments() throws Exception {
    // Create multiple segments by flushing between writes
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < 3; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("val", i * 100L));
        writer.addDocument(doc);
        writer.flush();
      }
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
    assertTrue(reader.leaves().size() >= 2, "Should have multiple segments");

    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("val", 0));
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(3, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("Batch size controls page size")
  void batchSizeControlsPageSize() throws Exception {
    writeDocuments(10);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));

    // Use batch size of 3
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns, 3);

    List<Page> pages = drainPages(scan);
    // Should have multiple pages: ceil(10/3) = 4
    assertTrue(pages.size() >= 3, "Should produce multiple pages with small batch");
    // Each page should have at most 3 rows
    for (Page page : pages) {
      assertTrue(page.getPositionCount() <= 3);
    }
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(10, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("Empty index produces no pages")
  void emptyIndex() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // Write nothing
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);

    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    assertTrue(scan.isFinished());
    assertNull(scan.getOutput());

    scan.close();
  }

  @Test
  @DisplayName("isBlocked returns NOT_BLOCKED")
  void isBlockedReturnsResolved() throws Exception {
    writeDocuments(1);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    assertTrue(scan.isBlocked().isDone());

    scan.close();
  }

  @Test
  @DisplayName("Source operator does not accept input")
  void sourceDoesNotAcceptInput() throws Exception {
    writeDocuments(1);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    assertFalse(scan.needsInput());
    assertThrows(UnsupportedOperationException.class, () -> scan.addInput(null));

    scan.close();
  }

  @Test
  @DisplayName("finish() makes operator finished")
  void finishMakesOperatorFinished() throws Exception {
    writeDocuments(5);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFullScan scan = new LuceneFullScan(createContext(), searcher, columns);

    assertFalse(scan.isFinished());
    scan.finish();
    assertTrue(scan.isFinished());
    assertNull(scan.getOutput());

    scan.close();
  }

  @Test
  @DisplayName("Factory creates operators correctly")
  void factoryCreatesOperator() throws Exception {
    writeDocuments(3);
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));

    LuceneFullScan.Factory factory = new LuceneFullScan.Factory(searcher, columns);
    LuceneFullScan op = (LuceneFullScan) factory.createOperator(createContext());

    List<Page> pages = drainPages(op);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(3, totalRows);

    op.close();
  }

  private List<Page> drainPages(LuceneFullScan scan) {
    List<Page> pages = new ArrayList<>();
    while (!scan.isFinished()) {
      Page page = scan.getOutput();
      if (page != null) {
        pages.add(page);
      }
    }
    return pages;
  }
}
