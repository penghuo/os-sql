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


import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sql.plugin.streamexpression.Tuple;
import org.opensearch.sql.plugin.streamexpression.comp.StreamComparator;
import org.opensearch.sql.plugin.streamexpression.stream.expr.DefaultStreamFactory;
import org.opensearch.sql.plugin.streamexpression.stream.expr.Explanation;
import org.opensearch.sql.plugin.streamexpression.stream.expr.Expressible;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamExpressionParameter;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamFactory;
import org.opensearch.sql.plugin.transport.StreamExpressionRequest;
import org.opensearch.sql.plugin.transport.StreamExpressionResponse;
import org.opensearch.sql.plugin.transport.TransportStreamExpressionAction;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;


public class ParallelStream extends TupleStream implements Expressible {

  private static final Logger LOG = LogManager.getLogger();

  private TupleStream tupleStream;
  private StreamFactory streamFactory;

  private ClusterService clusterService;
  private TransportService transportService;

  private CompletableFuture<Tuple> result;

  public ParallelStream(TupleStream stream) {
    this.tupleStream = stream;
    this.streamFactory = new DefaultStreamFactory();
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.clusterService = context.getClusterService();
    this.transportService = context.getTransportService();
  }

  @Override
  public List<TupleStream> children() {
    return Collections.singletonList(tupleStream);
  }

  @Override
  public void open() throws IOException {
    result = new CompletableFuture<>();
    clusterService.state().nodes().forEach(
        node -> {
          try {
            transportService.sendRequest(node, TransportStreamExpressionAction.ACTION_NAME,
                new StreamExpressionRequest(((Expressible) tupleStream).toExpression(streamFactory).toString()),
                new TransportResponseHandler<StreamExpressionResponse>() {
                  @Override
                  public void handleResponse(StreamExpressionResponse transportResponse) {
                    LOG.info("Action Response: {}", transportResponse.getResult());
                    result.complete(new Tuple("KEY", transportResponse.getResult()));
                  }

                  @Override
                  public void handleException(TransportException e) {
                    result.completeExceptionally(e);
                  }

                  @Override
                  public String executor() {
                    return TransportStreamExpressionAction.EXECUTOR;
                  }

                  @Override
                  public StreamExpressionResponse read(StreamInput streamInput) throws IOException {
                    return new StreamExpressionResponse(streamInput);
                  }
                });
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
    );
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public Tuple read() throws IOException {
    try{
      return result.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation("ParallelStream");
  }
}
