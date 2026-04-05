/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.transport.TransportService;

/**
 * Factory for creating {@link TransportExchangeClient} instances. Each exchange operator in a
 * downstream stage gets its own client to pull pages from upstream.
 */
public class TransportExchangeClientFactory {

  private final TransportService transportService;

  public TransportExchangeClientFactory(TransportService transportService) {
    this.transportService = transportService;
  }

  public TransportExchangeClient create() {
    return new TransportExchangeClient(transportService);
  }
}
