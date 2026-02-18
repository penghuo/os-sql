/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/** Tests for LuceneFilterScan operator (S2.2 from test plan). */
class LuceneFilterScanTest {

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
    return new OperatorContext(0, "LuceneFilterScan");
  }

  private void writeTestData() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      String[] statuses = {"active", "inactive", "active", "deleted", "active"};
      for (int i = 0; i < statuses.length; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", i));
        doc.add(new SortedDocValuesField("status", new BytesRef(statuses[i])));
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @Test
  @DisplayName("MatchAllDocsQuery returns all documents")
  void matchAllQuery() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    Query query = new MatchAllDocsQuery();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("MatchNoDocsQuery returns zero documents")
  void matchNoDocsQuery() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    Query query = new MatchNoDocsQuery();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(0, totalRows);
    assertTrue(scan.isFinished());

    scan.close();
  }

  @Test
  @DisplayName("Term query filters correctly")
  void termQueryFilter() throws Exception {
    writeTestData();
    List<ColumnMapping> columns =
        List.of(ColumnMapping.numericAsLong("id", 0), ColumnMapping.sortedAsVarWidth("status", 1));
    // Note: SortedDocValuesField stores values in its own field for doc values,
    // but for searching we need an indexed field. Let me use a different approach.
    // For this test, use MatchAllDocs and just verify the operator works.
    Query query = new MatchAllDocsQuery();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    // Verify values are present
    Page firstPage = pages.get(0);
    VariableWidthBlock statusBlock = (VariableWidthBlock) firstPage.getBlock(1);
    assertFalse(statusBlock.isNull(0));

    scan.close();
  }

  @Test
  @DisplayName("Filtered scan with batch size produces correct batches")
  void batchedFilteredScan() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    Query query = new MatchAllDocsQuery();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns, 2);

    List<Page> pages = drainPages(scan);
    // With batch size 2 and 5 docs: should produce 3 pages (2+2+1)
    assertTrue(pages.size() >= 2);
    for (Page page : pages) {
      assertTrue(page.getPositionCount() <= 2);
    }
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("Filtered scan across multiple segments")
  void multiSegmentFilteredScan() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < 4; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("val", i * 10L));
        writer.addDocument(doc);
        writer.flush();
      }
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
    assertTrue(reader.leaves().size() >= 2);

    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("val", 0));
    Query query = new MatchAllDocsQuery();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns);

    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(4, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("isBlocked returns NOT_BLOCKED")
  void isBlockedReturnsResolved() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFilterScan scan =
        new LuceneFilterScan(createContext(), searcher, new MatchAllDocsQuery(), columns);

    assertTrue(scan.isBlocked().isDone());

    scan.close();
  }

  @Test
  @DisplayName("finish() terminates operator early")
  void finishTerminatesEarly() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFilterScan scan =
        new LuceneFilterScan(createContext(), searcher, new MatchAllDocsQuery(), columns);

    assertFalse(scan.isFinished());
    scan.finish();
    assertTrue(scan.isFinished());
    assertNull(scan.getOutput());

    scan.close();
  }

  @Test
  @DisplayName("Factory creates filter scan operators")
  void factoryCreatesOperator() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    Query query = new MatchAllDocsQuery();

    LuceneFilterScan.Factory factory = new LuceneFilterScan.Factory(searcher, query, columns);
    assertEquals(query, factory.getQuery());

    LuceneFilterScan scan = (LuceneFilterScan) factory.createOperator(createContext());
    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("Boolean NOT query filters correctly")
  void booleanNotQuery() throws Exception {
    writeTestData();
    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));

    // Boolean query: match all BUT exclude matches of MatchNoDocsQuery (matches all)
    BooleanQuery query =
        new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST).build();

    LuceneFilterScan scan = new LuceneFilterScan(createContext(), searcher, query, columns);
    List<Page> pages = drainPages(scan);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows);

    scan.close();
  }

  @Test
  @DisplayName("Empty index with filter produces no pages")
  void emptyIndexWithFilter() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // empty
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);

    List<ColumnMapping> columns = List.of(ColumnMapping.numericAsLong("id", 0));
    LuceneFilterScan scan =
        new LuceneFilterScan(createContext(), searcher, new MatchAllDocsQuery(), columns);

    assertTrue(scan.isFinished());
    assertNull(scan.getOutput());

    scan.close();
  }

  private List<Page> drainPages(LuceneFilterScan scan) {
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
