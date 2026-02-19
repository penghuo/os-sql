/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.spill.FileSingleStreamSpiller;
import org.opensearch.sql.distributed.spill.SpillMetrics;
import org.opensearch.sql.distributed.spill.SpillerFactory;

/** Tests that spill files are properly cleaned up after query completes. */
class SpillCleanupTest {

  @TempDir Path tempDir;

  private Page testPage(long... values) {
    return new Page(new LongArrayBlock(values.length, Optional.empty(), values));
  }

  private long countSpillFiles() throws IOException {
    try (Stream<Path> files = Files.list(tempDir)) {
      return files.filter(p -> p.getFileName().toString().startsWith("spill-")).count();
    }
  }

  @Test
  @DisplayName("Spill files deleted after spiller close")
  void spillFilesDeletedAfterClose() throws Exception {
    PagesSerde serde = new PagesSerde();
    SpillMetrics metrics = new SpillMetrics();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);

    // Spill some pages
    spiller.spill(testPage(1, 2, 3));
    spiller.spill(testPage(4, 5, 6));

    // Verify spill file exists
    assertTrue(Files.exists(spiller.getSpillFile()), "Spill file should exist during processing");
    assertEquals(1, countSpillFiles(), "Should have exactly 1 spill file");

    // Close cleans up
    spiller.close();

    assertFalse(Files.exists(spiller.getSpillFile()), "Spill file should be deleted after close");
    assertEquals(0, countSpillFiles(), "No spill files should remain");
  }

  @Test
  @DisplayName("Spill cleanup after reading back pages")
  void spillCleanupAfterReadBack() throws Exception {
    PagesSerde serde = new PagesSerde();
    SpillMetrics metrics = new SpillMetrics();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);

    spiller.spill(testPage(10, 20, 30));
    spiller.spill(testPage(40, 50));

    // Read back pages
    List<Page> pages = spiller.getSpilledPagesList();
    assertEquals(2, pages.size());

    // Verify first page
    LongArrayBlock block0 = (LongArrayBlock) pages.get(0).getBlock(0);
    assertEquals(3, block0.getPositionCount());
    assertEquals(10L, block0.getLong(0));

    // Verify second page
    LongArrayBlock block1 = (LongArrayBlock) pages.get(1).getBlock(0);
    assertEquals(2, block1.getPositionCount());
    assertEquals(40L, block1.getLong(0));

    // Close deletes file
    spiller.close();
    assertEquals(0, countSpillFiles());
  }

  @Test
  @DisplayName("SpillerFactory creates spiller with correct directory")
  void spillerFactoryCreation() throws Exception {
    SpillerFactory factory = new SpillerFactory(tempDir);

    FileSingleStreamSpiller spiller = factory.create();
    assertNotNull(spiller);

    spiller.spill(testPage(1));
    assertTrue(Files.exists(spiller.getSpillFile()));
    assertTrue(spiller.getSpillFile().startsWith(tempDir));

    spiller.close();
    assertEquals(0, countSpillFiles());
  }

  @Test
  @DisplayName("Multiple spillers share directory without conflicts")
  void multipleSpillersShareDirectory() throws Exception {
    SpillerFactory factory = new SpillerFactory(tempDir);

    FileSingleStreamSpiller spiller1 = factory.create();
    FileSingleStreamSpiller spiller2 = factory.create();

    spiller1.spill(testPage(1, 2));
    spiller2.spill(testPage(3, 4));

    assertEquals(2, countSpillFiles(), "Should have 2 separate spill files");

    // Close spiller1, only its file removed
    spiller1.close();
    assertEquals(1, countSpillFiles(), "Only 1 spill file should remain");

    // Close spiller2, all cleaned up
    spiller2.close();
    assertEquals(0, countSpillFiles(), "All spill files should be cleaned up");
  }

  @Test
  @DisplayName("SpillMetrics tracks spill count and bytes")
  void spillMetricsTracking() throws Exception {
    SpillMetrics metrics = new SpillMetrics();
    PagesSerde serde = new PagesSerde();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);

    spiller.spill(testPage(1, 2, 3));
    spiller.spill(testPage(4, 5));

    assertEquals(2, spiller.getSpilledPageCount());
    assertTrue(spiller.getSpilledByteCount() > 0);

    // Read back triggers metrics recording
    spiller.getSpilledPagesList();

    spiller.close();
  }

  @Test
  @DisplayName("Double close is safe")
  void doubleCloseIsSafe() throws Exception {
    PagesSerde serde = new PagesSerde();
    SpillMetrics metrics = new SpillMetrics();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);
    spiller.spill(testPage(1));

    spiller.close();
    spiller.close(); // Should not throw

    assertEquals(0, countSpillFiles());
  }

  @Test
  @DisplayName("Cannot spill after close")
  void cannotSpillAfterClose() throws Exception {
    PagesSerde serde = new PagesSerde();
    SpillMetrics metrics = new SpillMetrics();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);
    spiller.close();

    assertThrows(IllegalStateException.class, () -> spiller.spill(testPage(1)));
  }

  @Test
  @DisplayName("Empty spiller produces empty iterator")
  void emptySpillerEmptyIterator() throws Exception {
    PagesSerde serde = new PagesSerde();
    SpillMetrics metrics = new SpillMetrics();

    FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(tempDir, serde, metrics);

    Iterator<Page> iter = spiller.getSpilledPages();
    assertFalse(iter.hasNext());

    spiller.close();
  }
}
