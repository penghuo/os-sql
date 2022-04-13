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
package org.opensearch.sql.opensearch.operator.stream;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.operator.common.SerDe;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionAction;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionRequest;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionResponse;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@EqualsAndHashCode
@ToString
public class ParallelStream extends PhysicalPlan {

  private static final Logger LOG = LogManager.getLogger();

  private final NodeClient client;
  private final GroupShardsIterator<ShardIterator> groupShardsIterator;
  private final PhysicalPlan input;

  private Iterator<ExprValue> iterator;

  public ParallelStream(NodeClient client, GroupShardsIterator<ShardIterator> groupShardsIterator,
                        PhysicalPlan input) {
    this.client = client;
    this.groupShardsIterator = groupShardsIterator;
    this.input = input;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitParallelStream(this, context);
  }

  @Override
  public void open() {
    List<CompletableFuture<ExprValue>> futureList = new ArrayList<>();


    for (ShardIterator shardIterator : groupShardsIterator) {
      ShardRouting shardRouting = shardIterator.nextOrNull();
      CompletableFuture<ExprValue> resp = new CompletableFuture<>();
      client.execute(
          StreamExpressionAction.INSTANCE,
          new StreamExpressionRequest(shardRouting, SerDe.serialize(input)),
          new ActionListener<>() {
            @Override
            public void onResponse(StreamExpressionResponse response) {
              resp.complete(SerDe.deserializeExprValue(response.getResult()));
            }

            @Override
            public void onFailure(Exception e) {
              resp.completeExceptionally(e);
            }
          }
      );
      futureList.add(resp);
    }

    List<ExprValue> exprValues = new ArrayList<>();
    try {
      for (CompletableFuture<ExprValue> future : futureList) {
        ExprValue exprValue = future.get();
        exprValues.addAll(exprValue.collectionValue());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    iterator = exprValues.listIterator();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(input);
  }
}
