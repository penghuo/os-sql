package org.opensearch.sql.flink.bear;

import java.util.concurrent.CompletableFuture;

public class BearService {

  public CompletableFuture<String> echo(String message) {
    return CompletableFuture.completedFuture(message);
  }
}
