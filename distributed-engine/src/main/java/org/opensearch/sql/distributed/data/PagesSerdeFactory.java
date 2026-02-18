/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

/** Factory for creating PagesSerde instances. */
public class PagesSerdeFactory {

  /** Creates a new PagesSerde instance. */
  public PagesSerde createPagesSerde() {
    return new PagesSerde();
  }
}
