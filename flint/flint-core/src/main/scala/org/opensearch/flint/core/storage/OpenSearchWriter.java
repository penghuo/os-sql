/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class OpenSearchWriter extends Writer {

  private static final String LINE_SEP = "\n";

  private final String index;

  private final RestHighLevelClient client;

  private final StringBuilder stringBuilder;

  private final List<String> documents;

  public OpenSearchWriter(RestHighLevelClient client, String index) {
    this.client = client;
    this.index = index;
    this.stringBuilder = new StringBuilder();
    this.documents = new ArrayList<>();
  }

  @Override
  public void write(char[] cbuf, int off, int len) {
    stringBuilder.append(cbuf, off, len);
  }

  @Override
  public void flush() throws IOException {
    String[] docs = stringBuilder.toString().split(LINE_SEP);

    if (docs.length == 0) {
      // do nothing
      return;
    }

    // Create bulk request
    BulkRequest request = new BulkRequest();
    for (String doc : docs) {
      request.add(new IndexRequest(index).source(doc, XContentType.JSON));
    }

    // Execute bulk request
    try {
      client.bulk(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IOException("Failed to execute bulk request", e);
    }

    documents.clear();
    stringBuilder.setLength(0);
  }

  @Override
  public void close() throws IOException {
    if (stringBuilder.length() > 0) {
      flush();
    }
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}


