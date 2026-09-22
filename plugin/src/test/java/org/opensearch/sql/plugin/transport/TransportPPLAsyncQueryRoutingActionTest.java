/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.node.NodeClosedException;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.RemoteTransportException;

public class TransportPPLAsyncQueryRoutingActionTest {

  @Test
  public void ownerConnectionFailuresAreUnavailableButApplicationFailuresAreNot() {
    DiscoveryNode owner = mock(DiscoveryNode.class);

    assertTrue(
        TransportPPLAsyncQueryRoutingAction.ownerUnavailable(
            new NodeDisconnectedException(owner, PPLAsyncGetResultAction.NAME)));
    assertTrue(
        TransportPPLAsyncQueryRoutingAction.ownerUnavailable(
            new RemoteTransportException("owner", new NodeClosedException(owner))));
    assertFalse(
        TransportPPLAsyncQueryRoutingAction.ownerUnavailable(
            new IllegalArgumentException("invalid request")));
  }
}
