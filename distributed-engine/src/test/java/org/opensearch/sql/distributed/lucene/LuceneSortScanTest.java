/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import static org.junit.jupiter.api.Assertions.*;

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
import org.apache.lucene.search.MatchAllDocsQuery;
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
import org.opensearch.sql.distributed.operator.SortOrder;

/** Tests for LuceneSortScan: sort ASC/DESC, keyword sort, top-K with early termination. */
class LuceneSortScanTest {

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

  private OperatorContext ctx() {
    return new OperatorContext(0, "LuceneSortScan");
  }

  private IndexSearcher writeSortTestData() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      long[] salaries = {50000, 120000, 80000, 30000, 95000};
      String[] names = {"Charlie", "Alice", "Eve", "Bob", "Dave"};
      for (int i = 0; i < salaries.length; i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("salary", salaries[i]));
        doc.add(new SortedDocValuesField("name", new BytesRef(names[i])));
        writer.addDocument(doc);
      }
    }
    reader = DirectoryReader.open(directory);
    return new IndexSearcher(reader);
  }

  /** Collects all long values from pages at given channel. */
  private long[] collectLongs(LuceneSortScan scan) {
    List<Long> values = new ArrayList<>();
    Page page;
    while ((page = scan.getOutput()) != null) {
      LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
      for (int i = 0; i < page.getPositionCount(); i++) {
        values.add(block.getLong(i));
      }
    }
    return values.stream().mapToLong(Long::longValue).toArray();
  }

  /** Collects all string values from pages at given channel. */
  private String[] collectStrings(LuceneSortScan scan, int channel) {
    List<String> values = new ArrayList<>();
    Page page;
    while ((page = scan.getOutput()) != null) {
      VariableWidthBlock block = (VariableWidthBlock) page.getBlock(channel);
      for (int i = 0; i < page.getPositionCount(); i++) {
        values.add(block.getString(i));
      }
    }
    return values.toArray(new String[0]);
  }

  // ==================== SORT ASC/DESC ====================

  @Test
  @DisplayName("Sort by long field ASC")
  void sortByLongFieldAsc() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 100);

    long[] salaries = collectLongs(scan);
    assertArrayEquals(new long[] {30000, 50000, 80000, 95000, 120000}, salaries);

    assertTrue(scan.isFinished());
    scan.close();
  }

  @Test
  @DisplayName("Sort by long field DESC")
  void sortByLongFieldDesc() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.DESC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 100);

    long[] salaries = collectLongs(scan);
    assertArrayEquals(new long[] {120000, 95000, 80000, 50000, 30000}, salaries);

    scan.close();
  }

  // ==================== KEYWORD SORT ====================

  @Test
  @DisplayName("Sort by keyword field ASC")
  void sortByKeywordFieldAsc() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping nameCol =
        new ColumnMapping(
            "name", ColumnMapping.DocValuesType.SORTED, ColumnMapping.BlockType.VARIABLE_WIDTH, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(nameCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(nameCol), sort, 100);

    String[] names = collectStrings(scan, 0);
    assertArrayEquals(new String[] {"Alice", "Bob", "Charlie", "Dave", "Eve"}, names);

    scan.close();
  }

  @Test
  @DisplayName("Sort by keyword field DESC")
  void sortByKeywordFieldDesc() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping nameCol =
        new ColumnMapping(
            "name", ColumnMapping.DocValuesType.SORTED, ColumnMapping.BlockType.VARIABLE_WIDTH, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(nameCol), List.of(SortOrder.DESC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(nameCol), sort, 100);

    String[] names = collectStrings(scan, 0);
    assertArrayEquals(new String[] {"Eve", "Dave", "Charlie", "Bob", "Alice"}, names);

    scan.close();
  }

  // ==================== TOP-K ====================

  @Test
  @DisplayName("Top-K with early termination: top 3 by salary DESC")
  void topKEarlyTermination() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.DESC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 3);

    long[] salaries = collectLongs(scan);
    assertEquals(3, salaries.length, "Top-3 should return exactly 3 rows");
    assertArrayEquals(new long[] {120000, 95000, 80000}, salaries);

    scan.close();
  }

  @Test
  @DisplayName("Top-K returns fewer rows than K when index is smaller")
  void topKFewerThanK() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 100);

    long[] salaries = collectLongs(scan);
    assertEquals(5, salaries.length, "Only 5 docs, should return all");

    scan.close();
  }

  // ==================== EDGE CASES ====================

  @Test
  @DisplayName("Empty index sorted scan")
  void emptyIndexSortedScan() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // empty
    }
    reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 100);

    long[] salaries = collectLongs(scan);
    assertEquals(0, salaries.length);
    assertTrue(scan.isFinished());

    scan.close();
  }

  @Test
  @DisplayName("Sort across multiple segments produces correct order")
  void multiSegmentSort() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (long val : new long[] {300, 100, 200}) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("val", val));
        writer.addDocument(doc);
        writer.flush();
        writer.commit();
      }
    }
    reader = DirectoryReader.open(directory);
    assertTrue(reader.leaves().size() >= 2, "Should have multiple segments");
    IndexSearcher searcher = new IndexSearcher(reader);

    ColumnMapping valCol =
        new ColumnMapping(
            "val", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(valCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(valCol), sort, 100);

    long[] values = collectLongs(scan);
    assertArrayEquals(new long[] {100, 200, 300}, values);

    scan.close();
  }

  @Test
  @DisplayName("Operator lifecycle contract")
  void operatorLifecycle() throws Exception {
    IndexSearcher searcher = writeSortTestData();

    ColumnMapping salaryCol =
        new ColumnMapping(
            "salary", ColumnMapping.DocValuesType.NUMERIC, ColumnMapping.BlockType.LONG, 0);

    org.apache.lucene.search.Sort sort =
        LuceneSortFieldConverter.convert(List.of(salaryCol), List.of(SortOrder.ASC_NULLS_LAST));

    LuceneSortScan scan =
        new LuceneSortScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(salaryCol), sort, 100);

    assertFalse(scan.isFinished());
    assertTrue(scan.isBlocked().isDone());

    // Drain all output
    while (scan.getOutput() != null) {
      // consume
    }

    assertTrue(scan.isFinished());
    scan.close();
  }
}
