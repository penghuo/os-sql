/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.task.TaskId;

public class QLDataReader implements Iterator<ExprValue> {

  private static final Logger LOG = LogManager.getLogger();

  @Getter private final Set<TaskId> sourceTaskIdList;

  private final NodeClient client;

  private final List<List<ExprValue>> valueList = new ArrayList<>();

  private CompletableFuture<Void> wait;

  private Iterator<ExprValue> iterator;

  public QLDataReader(Set<TaskId> sourceTaskIdList, NodeClient client) {
    this.sourceTaskIdList = sourceTaskIdList;
    this.client = client;
  }

  public void open() {
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
    for (TaskId taskId : sourceTaskIdList) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      futures.add(future);
      client.execute(
          QLDataAction.INSTANCE,
          new QLDataRequest(taskId),
          new ActionListener<>() {
            @Override
            public void onResponse(QLDataResponse qlDataResponse) {
              valueList.add(qlDataResponse.getValues());
              future.complete(true);
            }

            @Override
            public void onFailure(Exception e) {
              future.completeExceptionally(e);
            }
          });
    }

    wait = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  @SneakyThrows
  @Override
  public boolean hasNext() {
    // todo, should we skip latch check.
    wait.join();
    assert valueList.size() == sourceTaskIdList.size();

    iterator = valueList.stream().flatMap(List::stream).iterator();
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }
}
