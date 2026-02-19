/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
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
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/** Tests for LuceneAggScan: COUNT, SUM, AVG over DocValues, GROUP BY keyword field. */
class LuceneAggScanTest {

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
    return new OperatorContext(0, "LuceneAggScan");
  }

  private void addDoc(IndexWriter writer, long salary, String dept) throws IOException {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("salary", salary));
    doc.add(new SortedDocValuesField("dept", new BytesRef(dept)));
    writer.addDocument(doc);
  }

  /** Writes test documents: engineering=3 rows, marketing=2 rows. */
  private IndexSearcher writeAggTestData() throws IOException {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addDoc(writer, 100000, "engineering");
      addDoc(writer, 120000, "engineering");
      addDoc(writer, 80000, "engineering");
      addDoc(writer, 90000, "marketing");
      addDoc(writer, 110000, "marketing");
    }
    reader = DirectoryReader.open(directory);
    return new IndexSearcher(reader);
  }

  // ==================== COUNT ====================

  @Test
  @DisplayName("COUNT(*) over all documents")
  void countStarAllDocs() throws Exception {
    IndexSearcher searcher = writeAggTestData();

    DocValuesAccumulator countAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.COUNT,
            null,
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(countAcc), null);

    Page output = scan.getOutput();
    assertNotNull(output);
    assertEquals(1, output.getPositionCount(), "Global agg produces 1 row");

    LongArrayBlock countBlock = (LongArrayBlock) output.getBlock(0);
    assertEquals(5L, countBlock.getLong(0), "Should count all 5 documents");

    assertTrue(scan.isFinished());
    scan.close();
  }

  @Test
  @DisplayName("COUNT(*) GROUP BY dept")
  void countGroupByDept() throws Exception {
    IndexSearcher searcher = writeAggTestData();

    DocValuesAccumulator countAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.COUNT,
            null,
            ColumnMapping.DocValuesType.SORTED,
            ColumnMapping.BlockType.VARIABLE_WIDTH);

    LuceneAggScan.GroupBySpec groupBy =
        new LuceneAggScan.GroupBySpec(
            "dept", ColumnMapping.DocValuesType.SORTED, ColumnMapping.BlockType.VARIABLE_WIDTH);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(countAcc), groupBy);

    Page output = scan.getOutput();
    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "2 groups: engineering, marketing");

    // Verify group keys are keyword strings
    VariableWidthBlock keyBlock = (VariableWidthBlock) output.getBlock(0);
    LongArrayBlock countBlock = (LongArrayBlock) output.getBlock(1);

    // Sorted by ordinal, so alphabetical: engineering(3), marketing(2)
    String key0 = keyBlock.getString(0);
    String key1 = keyBlock.getString(1);
    long count0 = countBlock.getLong(0);
    long count1 = countBlock.getLong(1);

    assertEquals("engineering", key0);
    assertEquals(3L, count0);
    assertEquals("marketing", key1);
    assertEquals(2L, count1);

    scan.close();
  }

  // ==================== SUM ====================

  @Test
  @DisplayName("SUM(salary) over all documents")
  void sumSalaryAllDocs() throws Exception {
    IndexSearcher searcher = writeAggTestData();

    DocValuesAccumulator sumAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.SUM,
            "salary",
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(sumAcc), null);

    Page output = scan.getOutput();
    assertNotNull(output);

    DoubleArrayBlock sumBlock = (DoubleArrayBlock) output.getBlock(0);
    assertEquals(500000.0, sumBlock.getDouble(0), 0.01, "Sum of all salaries");

    scan.close();
  }

  @Test
  @DisplayName("SUM(salary) GROUP BY dept")
  void sumSalaryGroupByDept() throws Exception {
    IndexSearcher searcher = writeAggTestData();

    DocValuesAccumulator sumAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.SUM,
            "salary",
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan.GroupBySpec groupBy =
        new LuceneAggScan.GroupBySpec(
            "dept", ColumnMapping.DocValuesType.SORTED, ColumnMapping.BlockType.VARIABLE_WIDTH);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(sumAcc), groupBy);

    Page output = scan.getOutput();
    assertNotNull(output);
    assertEquals(2, output.getPositionCount());

    VariableWidthBlock keyBlock = (VariableWidthBlock) output.getBlock(0);
    DoubleArrayBlock sumBlock = (DoubleArrayBlock) output.getBlock(1);

    // engineering: 100k+120k+80k=300k, marketing: 90k+110k=200k
    assertEquals("engineering", keyBlock.getString(0));
    assertEquals(300000.0, sumBlock.getDouble(0), 0.01);
    assertEquals("marketing", keyBlock.getString(1));
    assertEquals(200000.0, sumBlock.getDouble(1), 0.01);

    scan.close();
  }

  // ==================== EDGE CASES ====================

  @Test
  @DisplayName("Aggregation on empty index")
  void emptyIndex() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      // empty
    }
    reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    DocValuesAccumulator countAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.COUNT,
            null,
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(countAcc), null);

    // Empty index → finished immediately
    assertTrue(scan.isFinished());
    assertNull(scan.getOutput());

    scan.close();
  }

  @Test
  @DisplayName("Aggregation across multiple segments")
  void multipleSegments() throws Exception {
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      addDoc(writer, 100, "a");
      writer.flush();
      writer.commit();
      addDoc(writer, 200, "a");
      writer.flush();
      writer.commit();
      addDoc(writer, 300, "b");
      writer.flush();
      writer.commit();
    }
    reader = DirectoryReader.open(directory);
    assertTrue(reader.leaves().size() >= 2, "Should have multiple segments");
    IndexSearcher searcher = new IndexSearcher(reader);

    DocValuesAccumulator sumAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.SUM,
            "salary",
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(sumAcc), null);

    Page output = scan.getOutput();
    assertNotNull(output);
    DoubleArrayBlock sumBlock = (DoubleArrayBlock) output.getBlock(0);
    assertEquals(600.0, sumBlock.getDouble(0), 0.01, "Sum across multiple segments");

    scan.close();
  }

  @Test
  @DisplayName("Operator lifecycle: isFinished, getOutput, close")
  void operatorLifecycle() throws Exception {
    IndexSearcher searcher = writeAggTestData();

    DocValuesAccumulator countAcc =
        new DocValuesAccumulator(
            DocValuesAccumulator.AggFunction.COUNT,
            null,
            ColumnMapping.DocValuesType.NUMERIC,
            ColumnMapping.BlockType.LONG);

    LuceneAggScan scan =
        new LuceneAggScan(ctx(), searcher, new MatchAllDocsQuery(), List.of(countAcc), null);

    assertFalse(scan.isFinished());
    assertTrue(scan.isBlocked().isDone());

    Page output = scan.getOutput();
    assertNotNull(output);
    assertTrue(scan.isFinished());
    assertNull(scan.getOutput(), "Second getOutput should be null");

    scan.close();
  }
}
