/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;

/**
 * Simplified port from Trino's FileSingleStreamSpiller. Writes Pages to a local temporary file
 * using {@link PagesSerde} serialization, then reads them back during the merge phase.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(spillDir, serde, metrics)) {
 *     spiller.spill(page1);
 *     spiller.spill(page2);
 *     Iterator<Page> pages = spiller.getSpilledPages();
 *     while (pages.hasNext()) { ... }
 * }
 * }</pre>
 *
 * <p>The spill file is automatically deleted on {@link #close()}.
 */
public class FileSingleStreamSpiller implements AutoCloseable {

  private static final int BUFFER_SIZE = 64 * 1024;

  private final Path spillFile;
  private final PagesSerde serde;
  private final SpillMetrics metrics;

  private DataOutputStream outputStream;
  private int spilledPageCount;
  private long spilledByteCount;
  private boolean closed;

  public FileSingleStreamSpiller(Path spillDirectory, PagesSerde serde, SpillMetrics metrics) {
    this.serde = serde;
    this.metrics = metrics;
    this.spilledPageCount = 0;
    this.spilledByteCount = 0;
    this.closed = false;

    try {
      Files.createDirectories(spillDirectory);
      this.spillFile = Files.createTempFile(spillDirectory, "spill-", ".bin");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create spill file", e);
    }
  }

  /**
   * Spills a single Page to the temp file.
   *
   * @param page the page to spill
   */
  public void spill(Page page) {
    if (closed) {
      throw new IllegalStateException("Spiller is closed");
    }

    try {
      ensureOutputStream();
      byte[] serialized = serde.serialize(page);
      outputStream.writeInt(serialized.length);
      outputStream.write(serialized);
      spilledPageCount++;
      spilledByteCount += serialized.length + Integer.BYTES;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to spill page", e);
    }
  }

  /**
   * Spills multiple Pages.
   *
   * @param pages the pages to spill
   */
  public void spill(Iterable<Page> pages) {
    for (Page page : pages) {
      spill(page);
    }
  }

  /**
   * Finalizes the spill stream and returns an iterator over the spilled pages. After calling this
   * method, no more pages can be spilled.
   *
   * @return an iterator over all spilled pages, read back from disk
   */
  public Iterator<Page> getSpilledPages() {
    closeOutputStream();
    if (spilledPageCount == 0) {
      return List.<Page>of().iterator();
    }
    metrics.recordSpill(spilledByteCount, spilledPageCount);
    return new SpilledPageIterator();
  }

  /**
   * Returns a list of all spilled pages read back from disk.
   *
   * @return list of deserialized pages
   */
  public List<Page> getSpilledPagesList() {
    Iterator<Page> iterator = getSpilledPages();
    List<Page> pages = new ArrayList<>();
    while (iterator.hasNext()) {
      pages.add(iterator.next());
    }
    return pages;
  }

  public int getSpilledPageCount() {
    return spilledPageCount;
  }

  public long getSpilledByteCount() {
    return spilledByteCount;
  }

  public Path getSpillFile() {
    return spillFile;
  }

  private void ensureOutputStream() throws IOException {
    if (outputStream == null) {
      outputStream =
          new DataOutputStream(
              new BufferedOutputStream(new FileOutputStream(spillFile.toFile()), BUFFER_SIZE));
    }
  }

  private void closeOutputStream() {
    if (outputStream != null) {
      try {
        outputStream.flush();
        outputStream.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close spill output stream", e);
      }
      outputStream = null;
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    closeOutputStream();

    // Clean up the spill file
    try {
      Files.deleteIfExists(spillFile);
    } catch (IOException e) {
      // Best effort cleanup — log but don't throw
    }
  }

  /** Reads back spilled pages from the temp file. */
  private class SpilledPageIterator implements Iterator<Page> {
    private final DataInputStream inputStream;
    private int pagesRead;
    private boolean exhausted;

    SpilledPageIterator() {
      try {
        this.inputStream =
            new DataInputStream(
                new BufferedInputStream(new FileInputStream(spillFile.toFile()), BUFFER_SIZE));
        this.pagesRead = 0;
        this.exhausted = false;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to open spill file for reading", e);
      }
    }

    @Override
    public boolean hasNext() {
      return !exhausted && pagesRead < spilledPageCount;
    }

    @Override
    public Page next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        int length = inputStream.readInt();
        byte[] data = new byte[length];
        inputStream.readFully(data);
        Page page = serde.deserialize(data);
        pagesRead++;
        metrics.recordReadBack(length + Integer.BYTES, 1);

        if (pagesRead >= spilledPageCount) {
          exhausted = true;
          inputStream.close();
        }
        return page;
      } catch (IOException e) {
        exhausted = true;
        try {
          inputStream.close();
        } catch (IOException closeEx) {
          // suppress
        }
        throw new UncheckedIOException("Failed to read spilled page", e);
      }
    }
  }
}
