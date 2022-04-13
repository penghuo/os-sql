/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.stream;

import java.io.Serializable;
import lombok.Data;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.transport.TransportService;

@Data
public class StreamContext implements Serializable {
  private ClusterService clusterService;
  private TransportService transportService;
}
