/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;

/**
 * Response from a data node containing the Pages produced by executing a plan fragment against
 * local shards. Pages are serialized via {@link PagesSerde}.
 */
public class ShardQueryResponse extends ActionResponse {

  private final List<byte[]> serializedPages;
  private final boolean hasMore;

  /** Construct from already-serialized pages. */
  public ShardQueryResponse(List<byte[]> serializedPages, boolean hasMore) {
    this.serializedPages = List.copyOf(serializedPages);
    this.hasMore = hasMore;
  }

  /** Construct from Pages using the provided serde. */
  public ShardQueryResponse(List<Page> pages, boolean hasMore, PagesSerde serde) {
    List<byte[]> serialized = new ArrayList<>(pages.size());
    for (Page page : pages) {
      serialized.add(serde.serialize(page));
    }
    this.serializedPages = List.copyOf(serialized);
    this.hasMore = hasMore;
  }

  public ShardQueryResponse(StreamInput in) throws IOException {
    super(in);
    int pageCount = in.readVInt();
    List<byte[]> pages = new ArrayList<>(pageCount);
    for (int i = 0; i < pageCount; i++) {
      pages.add(in.readByteArray());
    }
    this.serializedPages = List.copyOf(pages);
    this.hasMore = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeVInt(serializedPages.size());
    for (byte[] page : serializedPages) {
      out.writeByteArray(page);
    }
    out.writeBoolean(hasMore);
  }

  /** Returns the serialized Pages as byte arrays. */
  public List<byte[]> getSerializedPages() {
    return serializedPages;
  }

  /** Deserialize the Pages using the provided serde. */
  public List<Page> getPages(PagesSerde serde) {
    List<Page> pages = new ArrayList<>(serializedPages.size());
    for (byte[] data : serializedPages) {
      pages.add(serde.deserialize(data));
    }
    return pages;
  }

  /** Returns true if there are more pages to fetch (streaming support). */
  public boolean hasMore() {
    return hasMore;
  }

  /** Returns the number of pages in this response. */
  public int getPageCount() {
    return serializedPages.size();
  }
}
