/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.opensearch.sql.distributed.data.PagesSerde;

/**
 * Factory for creating {@link FileSingleStreamSpiller} instances. Configures the spill directory
 * path and provides shared {@link PagesSerde} and {@link SpillMetrics} instances.
 *
 * <p>Default spill directory is {@code java.io.tmpdir/opensearch-sql-spill/}.
 */
public class SpillerFactory {

  private static final String DEFAULT_SPILL_SUBDIR = "opensearch-sql-spill";

  private final Path spillDirectory;
  private final PagesSerde serde;
  private final SpillMetrics metrics;

  public SpillerFactory() {
    this(Paths.get(System.getProperty("java.io.tmpdir"), DEFAULT_SPILL_SUBDIR));
  }

  public SpillerFactory(Path spillDirectory) {
    this(spillDirectory, new PagesSerde(), new SpillMetrics());
  }

  public SpillerFactory(Path spillDirectory, PagesSerde serde, SpillMetrics metrics) {
    this.spillDirectory = spillDirectory;
    this.serde = serde;
    this.metrics = metrics;
  }

  /**
   * Creates a new spiller instance.
   *
   * @return a new FileSingleStreamSpiller
   */
  public FileSingleStreamSpiller create() {
    return new FileSingleStreamSpiller(spillDirectory, serde, metrics);
  }

  public Path getSpillDirectory() {
    return spillDirectory;
  }

  public SpillMetrics getMetrics() {
    return metrics;
  }
}
