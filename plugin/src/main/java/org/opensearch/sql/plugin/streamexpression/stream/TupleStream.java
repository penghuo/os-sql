/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.plugin.streamexpression.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.plugin.streamexpression.Tuple;
import org.opensearch.sql.plugin.streamexpression.comp.StreamComparator;
import org.opensearch.sql.plugin.streamexpression.stream.expr.Explanation;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamFactory;
import org.opensearch.sql.plugin.transport.StreamExpressionResponse;
import org.opensearch.transport.TransportService;


/**
 * @since 5.1.0
 */
public abstract class TupleStream implements Closeable, Serializable {

  private static final long serialVersionUID = 1;

  private UUID streamNodeId = UUID.randomUUID();

  public TupleStream() {}

  public abstract void setStreamContext(StreamContext context);

  public abstract List<TupleStream> children();

  public abstract void open() throws IOException;

  public abstract void close() throws IOException;

  public abstract Tuple read() throws IOException;

  public abstract StreamComparator getStreamSort();

  public abstract Explanation toExplanation(StreamFactory factory) throws IOException;

  public void execute(ClusterService clusterService,
                      TransportService transportService,
                      ActionListener<StreamExpressionResponse> actionListener) {
    try {
      final StreamContext streamContext = new StreamContext();
      streamContext.setTransportService(transportService);
      streamContext.setClusterService(clusterService);
      setStreamContext(streamContext);
      open();
      Tuple tuple = read();
      actionListener.onResponse(new StreamExpressionResponse(tuple.getString("KEY")));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }
  }
}
