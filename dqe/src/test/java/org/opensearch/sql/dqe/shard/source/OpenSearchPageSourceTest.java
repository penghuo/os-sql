/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

@ExtendWith(MockitoExtension.class)
@DisplayName("OpenSearchPageSource: shard-local SearchAction -> Trino Pages")
class OpenSearchPageSourceTest {

  private static final String INDEX = "test_index";
  private static final int SHARD_ID = 0;
  private static final int BATCH_SIZE = 1024;

  @Test
  @DisplayName("SearchRequest uses _only_local preference")
  void usesOnlyLocalPreference() {
    // Arrange: capture the SearchRequest passed to the client
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    SearchResponse searchResponse = mockSearchResponse(new SearchHit[0], null);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(captor.capture())).thenReturn(searchResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("name", VarcharType.VARCHAR));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act
    source.processNextBatch();

    // Assert
    SearchRequest captured = captor.getValue();
    assertEquals("_only_local", captured.preference());
  }

  @Test
  @DisplayName("Converts SearchHits to Trino Page with correct values")
  void convertsHitsToPage() {
    // Arrange: 2 hits with name and age fields
    SearchHit hit0 = createSearchHit(0, "{\"name\":\"alice\",\"age\":30}");
    SearchHit hit1 = createSearchHit(1, "{\"name\":\"bob\",\"age\":25}");
    SearchResponse searchResponse = mockSearchResponse(new SearchHit[] {hit0, hit1}, "scroll_abc");

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(searchResponse);

    List<ColumnHandle> columns =
        List.of(
            new ColumnHandle("name", VarcharType.VARCHAR),
            new ColumnHandle("age", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act
    Page page = source.processNextBatch();

    // Assert
    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
    assertEquals(2, page.getChannelCount());
    // Verify bigint values
    assertEquals(30L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
    assertEquals(25L, BigintType.BIGINT.getLong(page.getBlock(1), 1));
    // Verify varchar values
    assertEquals("alice", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
    assertEquals("bob", VarcharType.VARCHAR.getSlice(page.getBlock(0), 1).toStringUtf8());
  }

  @Test
  @DisplayName("Returns null when no hits returned on first request")
  void returnsNullWhenExhaustedOnFirstRequest() {
    // Arrange: empty response
    SearchResponse searchResponse = mockSearchResponse(new SearchHit[0], null);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(searchResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act & Assert
    assertNull(source.processNextBatch());
  }

  @Test
  @DisplayName("Returns null on second call when scroll returns empty hits")
  void returnsNullWhenScrollExhausted() {
    // Arrange: first call returns data, second (scroll) returns empty
    SearchHit hit0 = createSearchHit(0, "{\"val\":42}");
    SearchResponse firstResponse = mockSearchResponse(new SearchHit[] {hit0}, "scroll_123");
    SearchResponse scrollResponse = mockSearchResponse(new SearchHit[0], null);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(firstResponse);
    when(client.searchScroll(any(SearchScrollRequest.class))).thenReturn(scrollResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act
    Page page1 = source.processNextBatch();
    assertNotNull(page1);
    Page page2 = source.processNextBatch();

    // Assert
    assertNull(page2);
  }

  @Test
  @DisplayName("Uses scroll API for second batch")
  void usesScrollForSubsequentBatches() {
    // Arrange: two batches of data
    SearchHit hit0 = createSearchHit(0, "{\"val\":1}");
    SearchHit hit1 = createSearchHit(1, "{\"val\":2}");
    SearchResponse firstResponse = mockSearchResponse(new SearchHit[] {hit0}, "scroll_abc");
    SearchResponse scrollResponse = mockSearchResponse(new SearchHit[] {hit1}, null);
    SearchResponse emptyResponse = mockSearchResponse(new SearchHit[0], null);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(firstResponse);
    when(client.searchScroll(any(SearchScrollRequest.class)))
        .thenReturn(scrollResponse)
        .thenReturn(emptyResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act
    Page page1 = source.processNextBatch();
    Page page2 = source.processNextBatch();
    Page page3 = source.processNextBatch();

    // Assert
    assertNotNull(page1);
    assertEquals(1L, BigintType.BIGINT.getLong(page1.getBlock(0), 0));
    assertNotNull(page2);
    assertEquals(2L, BigintType.BIGINT.getLong(page2.getBlock(0), 0));
    assertNull(page3);

    // Verify scroll was used for subsequent batches (2 scroll calls: page2 and page3)
    ArgumentCaptor<SearchScrollRequest> scrollCaptor =
        ArgumentCaptor.forClass(SearchScrollRequest.class);
    verify(client, times(2)).searchScroll(scrollCaptor.capture());
    // All scroll requests should use the same scroll ID
    for (SearchScrollRequest req : scrollCaptor.getAllValues()) {
      assertEquals("scroll_abc", req.scrollId());
    }
  }

  @Test
  @DisplayName("Clears scroll on close when scroll was established")
  void clearsScrollOnClose() {
    // Arrange: establish a scroll context
    SearchHit hit0 = createSearchHit(0, "{\"val\":1}");
    SearchResponse searchResponse = mockSearchResponse(new SearchHit[] {hit0}, "scroll_xyz");
    ClearScrollResponse clearResponse = mock(ClearScrollResponse.class);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(searchResponse);
    when(client.clearScroll(any(ClearScrollRequest.class))).thenReturn(clearResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Trigger first search to establish scroll
    source.processNextBatch();

    // Act
    source.close();

    // Assert: clearScroll was called with the correct scroll ID
    ArgumentCaptor<ClearScrollRequest> captor = ArgumentCaptor.forClass(ClearScrollRequest.class);
    verify(client).clearScroll(captor.capture());
    assertEquals(List.of("scroll_xyz"), captor.getValue().getScrollIds());
  }

  @Test
  @DisplayName("Does not call clearScroll on close when no scroll was established")
  void doesNotClearScrollWhenNeverSearched() {
    // Arrange: never search, just close
    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act
    source.close();

    // Assert
    verify(client, never()).clearScroll(any());
  }

  @Test
  @DisplayName("Repeated calls after exhaustion keep returning null")
  void repeatedCallsAfterExhaustionReturnNull() {
    SearchResponse searchResponse = mockSearchResponse(new SearchHit[0], null);

    OpenSearchPageSource.SearchClient client = mock(OpenSearchPageSource.SearchClient.class);
    when(client.search(any(SearchRequest.class))).thenReturn(searchResponse);

    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    SearchSourceBuilder dslQuery = new SearchSourceBuilder();

    OpenSearchPageSource source =
        new OpenSearchPageSource(client, INDEX, SHARD_ID, dslQuery, columns, BATCH_SIZE);

    // Act & Assert
    assertNull(source.processNextBatch());
    assertNull(source.processNextBatch());
    assertNull(source.processNextBatch());
  }

  // --- Helper methods ---

  /** Create a real SearchHit with the given JSON source. */
  private SearchHit createSearchHit(int docId, String jsonSource) {
    SearchHit hit = new SearchHit(docId);
    hit.sourceRef(new BytesArray(jsonSource));
    return hit;
  }

  /**
   * Create a mock SearchResponse with the given hits and scroll ID. Only stubs {@code
   * getScrollId()} when scrollId is non-null, to avoid unnecessary stubbing warnings from strict
   * Mockito.
   */
  private SearchResponse mockSearchResponse(SearchHit[] hits, String scrollId) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits searchHits =
        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
    when(response.getHits()).thenReturn(searchHits);
    if (scrollId != null) {
      when(response.getScrollId()).thenReturn(scrollId);
    }
    return response;
  }
}
