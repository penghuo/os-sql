/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.parser.DqeException;

class GatherExchangeSinkTests {

  private Page createPage(int rows) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(builder.build());
  }

  @Test
  @DisplayName("addPage accumulates pages until flush on finish")
  void addPageAccumulatesUntilFinish() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);

    sink.addPage(createPage(10));
    sink.addPage(createPage(20));
    // No chunk sent yet (under max chunk size)
    assertEquals(0, sentChunks.size());

    sink.finish();
    assertEquals(1, sentChunks.size());
    assertTrue(sentChunks.get(0).isLast());
    assertEquals(2, sentChunks.get(0).getPages().size());
    assertEquals(0, sentChunks.get(0).getSequenceNumber());
  }

  @Test
  @DisplayName("addPage auto-flushes when chunk size exceeded")
  void addPageAutoFlushes() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    // Max chunk size of 1 byte — every page triggers a flush
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1, sentChunks::add);

    sink.addPage(createPage(10));
    assertEquals(1, sentChunks.size());
    assertFalse(sentChunks.get(0).isLast());
    assertEquals(0, sentChunks.get(0).getSequenceNumber());

    sink.addPage(createPage(20));
    assertEquals(2, sentChunks.size());
    assertEquals(1, sentChunks.get(1).getSequenceNumber());

    sink.finish();
    // Finish sends an empty last chunk
    assertEquals(3, sentChunks.size());
    assertTrue(sentChunks.get(2).isLast());
  }

  @Test
  @DisplayName("finish is idempotent")
  void finishIsIdempotent() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);
    sink.addPage(createPage(5));
    sink.finish();
    sink.finish(); // second call is no-op
    assertEquals(1, sentChunks.size());
  }

  @Test
  @DisplayName("addPage after finish throws")
  void addPageAfterFinishThrows() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);
    sink.finish();
    assertThrows(DqeException.class, () -> sink.addPage(createPage(5)));
  }

  @Test
  @DisplayName("addPage after abort throws")
  void addPageAfterAbortThrows() {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);
    sink.abort();
    assertThrows(DqeException.class, () -> sink.addPage(createPage(5)));
  }

  @Test
  @DisplayName("abort clears pending pages")
  void abortClearsPending() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);
    sink.addPage(createPage(10));
    sink.abort();
    assertEquals(0, sentChunks.size());
  }

  @Test
  @DisplayName("sequence number increments correctly")
  void sequenceNumberIncrements() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1, sentChunks::add);

    assertEquals(0, sink.getSequenceNumber());
    sink.addPage(createPage(5));
    assertEquals(1, sink.getSequenceNumber());
    sink.addPage(createPage(5));
    assertEquals(2, sink.getSequenceNumber());
    sink.finish();
    assertEquals(3, sink.getSequenceNumber());
  }

  @Test
  @DisplayName("close without finish aborts")
  void closeWithoutFinishAborts() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, sentChunks::add);
    sink.addPage(createPage(10));
    sink.close(); // should abort, not finish
    assertEquals(0, sentChunks.size());
  }

  @Test
  @DisplayName("chunk metadata is correct")
  void chunkMetadataCorrect() throws DqeException {
    List<DqeExchangeChunk> sentChunks = new ArrayList<>();
    GatherExchangeSink sink = new GatherExchangeSink("q42", 3, 7, 1024 * 1024, sentChunks::add);
    sink.addPage(createPage(5));
    sink.finish();

    DqeExchangeChunk chunk = sentChunks.get(0);
    assertEquals("q42", chunk.getQueryId());
    assertEquals(3, chunk.getStageId());
    assertEquals(7, chunk.getPartitionId());
  }

  @Test
  @DisplayName("addPage rejects null page")
  void addPageRejectsNull() {
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024, chunk -> {});
    assertThrows(NullPointerException.class, () -> sink.addPage(null));
  }

  @Test
  @DisplayName("isFinished reflects state correctly")
  void isFinishedReflectsState() throws DqeException {
    GatherExchangeSink sink = new GatherExchangeSink("q1", 0, 0, 1024 * 1024, chunk -> {});
    assertFalse(sink.isFinished());
    sink.finish();
    assertTrue(sink.isFinished());
  }
}
