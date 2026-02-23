/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypes;
import org.opensearch.dqe.types.mapping.DateFormatResolver;
import org.opensearch.dqe.types.mapping.MultiFieldResolver;
import org.opensearch.dqe.types.mapping.OpenSearchTypeMappingResolver;

/** Comprehensive tests for the dqe-metadata module (M-1 through M-7). */
class DqeMetadataTests {

  // ---------------------------------------------------------------------------
  // M-1: DqeTableHandle tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-1: DqeTableHandle")
  class TableHandleTests {

    @Test
    @DisplayName("basic construction and getters")
    void testBasicConstruction() {
      DqeTableHandle handle =
          new DqeTableHandle("my_index", "my_*", List.of("my_index_1", "my_index_2"), 42L, null);

      assertEquals("my_index", handle.getIndexName());
      assertEquals("my_*", handle.getIndexPattern());
      assertEquals(List.of("my_index_1", "my_index_2"), handle.getResolvedIndices());
      assertEquals(42L, handle.getSchemaGeneration());
      assertNull(handle.getPitId());
    }

    @Test
    @DisplayName("null resolvedIndices defaults to single-element list with indexName")
    void testNullResolvedIndices() {
      DqeTableHandle handle = new DqeTableHandle("idx", null, null, 1L, null);
      assertEquals(List.of("idx"), handle.getResolvedIndices());
    }

    @Test
    @DisplayName("withPitId creates new handle with PIT ID")
    void testWithPitId() {
      DqeTableHandle original = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      DqeTableHandle withPit = original.withPitId("pit-123");

      assertNull(original.getPitId());
      assertEquals("pit-123", withPit.getPitId());
      assertEquals("idx", withPit.getIndexName());
      assertEquals(1L, withPit.getSchemaGeneration());
    }

    @Test
    @DisplayName("equality based on indexName and schemaGeneration")
    void testEquality() {
      DqeTableHandle a = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      DqeTableHandle b = new DqeTableHandle("idx", "pat*", List.of("idx"), 1L, "pit-1");
      DqeTableHandle c = new DqeTableHandle("other", null, List.of("other"), 1L, null);
      DqeTableHandle d = new DqeTableHandle("idx", null, List.of("idx"), 2L, null);

      assertEquals(a, b); // same indexName + generation
      assertNotEquals(a, c); // different indexName
      assertNotEquals(a, d); // different generation
      assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    @DisplayName("toString includes relevant info")
    void testToString() {
      DqeTableHandle handle = new DqeTableHandle("idx", "pat*", List.of("idx"), 5L, "pit-1");
      String str = handle.toString();
      assertThat(str, containsString("idx"));
      assertThat(str, containsString("pattern=pat*"));
      assertThat(str, containsString("gen=5"));
      assertThat(str, containsString("pit=true"));
    }

    @Test
    @DisplayName("Writeable round-trip serialization")
    void testWriteableRoundTrip() throws IOException {
      DqeTableHandle original =
          new DqeTableHandle("my_idx", "my_*", List.of("my_idx_1", "my_idx_2"), 7L, "pit-abc");

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeTableHandle deserialized = new DqeTableHandle(in);

      assertEquals(original.getIndexName(), deserialized.getIndexName());
      assertEquals(original.getIndexPattern(), deserialized.getIndexPattern());
      assertEquals(original.getResolvedIndices(), deserialized.getResolvedIndices());
      assertEquals(original.getSchemaGeneration(), deserialized.getSchemaGeneration());
      assertEquals(original.getPitId(), deserialized.getPitId());
    }

    @Test
    @DisplayName("Writeable round-trip with null optional fields")
    void testWriteableRoundTripNulls() throws IOException {
      DqeTableHandle original = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeTableHandle deserialized = new DqeTableHandle(in);

      assertNull(deserialized.getIndexPattern());
      assertNull(deserialized.getPitId());
    }

    @Test
    @DisplayName("null indexName throws NPE")
    void testNullIndexNameThrows() {
      assertThrows(
          NullPointerException.class, () -> new DqeTableHandle(null, null, List.of(), 1L, null));
    }
  }

  // ---------------------------------------------------------------------------
  // M-1: DqeColumnHandle tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-1: DqeColumnHandle")
  class ColumnHandleTests {

    @Test
    @DisplayName("basic construction and getters")
    void testBasicConstruction() {
      DqeColumnHandle col =
          new DqeColumnHandle("city", "address.city", DqeTypes.VARCHAR, true, null, false);

      assertEquals("city", col.getFieldName());
      assertEquals("address.city", col.getFieldPath());
      assertEquals(DqeTypes.VARCHAR, col.getType());
      assertTrue(col.isSortable());
      assertNull(col.getKeywordSubField());
      assertFalse(col.isArray());
    }

    @Test
    @DisplayName("construction with keyword sub-field and array flag")
    void testWithKeywordAndArray() {
      DqeColumnHandle col =
          new DqeColumnHandle("title", "title", DqeTypes.VARCHAR, false, "title.keyword", true);

      assertEquals("title.keyword", col.getKeywordSubField());
      assertTrue(col.isArray());
    }

    @Test
    @DisplayName("equality based on fieldPath and type")
    void testEquality() {
      DqeColumnHandle a = new DqeColumnHandle("f", "path.f", DqeTypes.BIGINT, true, null, false);
      DqeColumnHandle b = new DqeColumnHandle("f", "path.f", DqeTypes.BIGINT, false, "kw", true);
      DqeColumnHandle c = new DqeColumnHandle("f", "path.f", DqeTypes.VARCHAR, true, null, false);
      DqeColumnHandle d = new DqeColumnHandle("f", "other.f", DqeTypes.BIGINT, true, null, false);

      assertEquals(a, b); // same fieldPath + type
      assertNotEquals(a, c); // different type
      assertNotEquals(a, d); // different fieldPath
    }

    @Test
    @DisplayName("Writeable round-trip for VARCHAR column")
    void testWriteableVarchar() throws IOException {
      DqeColumnHandle original =
          new DqeColumnHandle(
              "name", "user.name", DqeTypes.VARCHAR, true, "user.name.keyword", false);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeColumnHandle deserialized = new DqeColumnHandle(in);

      assertEquals(original.getFieldName(), deserialized.getFieldName());
      assertEquals(original.getFieldPath(), deserialized.getFieldPath());
      assertEquals(original.getType(), deserialized.getType());
      assertEquals("user.name.keyword", deserialized.getKeywordSubField());
    }

    @Test
    @DisplayName("Writeable round-trip for BIGINT column")
    void testWriteableBigint() throws IOException {
      DqeColumnHandle original =
          new DqeColumnHandle("age", "age", DqeTypes.BIGINT, true, null, false);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeColumnHandle deserialized = new DqeColumnHandle(in);

      assertEquals(DqeTypes.BIGINT, deserialized.getType());
    }

    @Test
    @DisplayName("Writeable round-trip for DECIMAL column")
    void testWriteableDecimal() throws IOException {
      DqeType decimalType = DqeTypes.decimal(20, 0);
      DqeColumnHandle original =
          new DqeColumnHandle("amount", "amount", decimalType, true, null, false);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeColumnHandle deserialized = new DqeColumnHandle(in);

      assertEquals(decimalType, deserialized.getType());
    }

    @Test
    @DisplayName("Writeable round-trip for TIMESTAMP column")
    void testWriteableTimestamp() throws IOException {
      DqeColumnHandle original =
          new DqeColumnHandle("ts", "ts", DqeTypes.TIMESTAMP_MILLIS, true, null, false);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeColumnHandle deserialized = new DqeColumnHandle(in);

      assertEquals(DqeTypes.TIMESTAMP_MILLIS, deserialized.getType());
    }

    @Test
    @DisplayName("Writeable round-trip for BOOLEAN column with array flag")
    void testWriteableBooleanArray() throws IOException {
      DqeColumnHandle original =
          new DqeColumnHandle("flags", "flags", DqeTypes.BOOLEAN, true, null, true);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeColumnHandle deserialized = new DqeColumnHandle(in);

      assertTrue(deserialized.isArray());
      assertEquals(DqeTypes.BOOLEAN, deserialized.getType());
    }

    @Test
    @DisplayName("toString includes type and array marker")
    void testToString() {
      DqeColumnHandle col =
          new DqeColumnHandle("tags", "tags", DqeTypes.VARCHAR, false, null, true);
      String str = col.toString();
      assertThat(str, containsString("tags"));
      assertThat(str, containsString("VARCHAR"));
      assertThat(str, containsString("ARRAY"));
    }
  }

  // ---------------------------------------------------------------------------
  // M-1: DqeShardSplit tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-1: DqeShardSplit")
  class ShardSplitTests {

    @Test
    @DisplayName("basic construction and getters")
    void testBasicConstruction() {
      DqeShardSplit split = new DqeShardSplit(0, "node-1", "my_index", true);

      assertEquals(0, split.getShardId());
      assertEquals("node-1", split.getNodeId());
      assertEquals("my_index", split.getIndexName());
      assertTrue(split.isPrimary());
    }

    @Test
    @DisplayName("equality based on shardId and indexName")
    void testEquality() {
      DqeShardSplit a = new DqeShardSplit(0, "node-1", "idx", true);
      DqeShardSplit b = new DqeShardSplit(0, "node-2", "idx", false);
      DqeShardSplit c = new DqeShardSplit(1, "node-1", "idx", true);
      DqeShardSplit d = new DqeShardSplit(0, "node-1", "other", true);

      assertEquals(a, b); // same shardId + indexName
      assertNotEquals(a, c); // different shardId
      assertNotEquals(a, d); // different indexName
    }

    @Test
    @DisplayName("Writeable round-trip serialization")
    void testWriteableRoundTrip() throws IOException {
      DqeShardSplit original = new DqeShardSplit(3, "node-abc", "test_idx", false);

      BytesStreamOutput out = new BytesStreamOutput();
      original.writeTo(out);

      StreamInput in = out.bytes().streamInput();
      DqeShardSplit deserialized = new DqeShardSplit(in);

      assertEquals(original.getShardId(), deserialized.getShardId());
      assertEquals(original.getNodeId(), deserialized.getNodeId());
      assertEquals(original.getIndexName(), deserialized.getIndexName());
      assertEquals(original.isPrimary(), deserialized.isPrimary());
    }

    @Test
    @DisplayName("toString includes all fields")
    void testToString() {
      DqeShardSplit split = new DqeShardSplit(2, "node-x", "idx", true);
      String str = split.toString();
      assertThat(str, containsString("idx"));
      assertThat(str, containsString("[2]"));
      assertThat(str, containsString("primary"));
      assertThat(str, containsString("node-x"));
    }
  }

  // ---------------------------------------------------------------------------
  // M-4: ShardSelector tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-4: ShardSelector")
  class ShardSelectorTests {

    @Test
    @DisplayName("prefers started primary")
    void testPrefersStartedPrimary() {
      ShardRouting primary = mockShardRouting("node-1", true, true);
      ShardRouting replica = mockShardRouting("node-2", false, true);

      IndexShardRoutingTable table = mockShardRoutingTable(primary, List.of(primary, replica));

      ShardSelector selector = new ShardSelector("node-3");
      ShardRouting selected = selector.select(table);

      assertThat(selected, sameInstance(primary));
    }

    @Test
    @DisplayName("falls back to local replica when primary not started")
    void testFallsBackToLocalReplica() {
      ShardRouting primary = mockShardRouting("node-1", true, false);
      ShardRouting remoteReplica = mockShardRouting("node-2", false, true);
      ShardRouting localReplica = mockShardRouting("local-node", false, true);

      IndexShardRoutingTable table =
          mockShardRoutingTable(primary, List.of(primary, remoteReplica, localReplica));

      ShardSelector selector = new ShardSelector("local-node");
      ShardRouting selected = selector.select(table);

      assertThat(selected, sameInstance(localReplica));
    }

    @Test
    @DisplayName("falls back to any started replica when primary not started and no local replica")
    void testFallsBackToAnyReplica() {
      ShardRouting primary = mockShardRouting("node-1", true, false);
      ShardRouting replica = mockShardRouting("node-2", false, true);

      IndexShardRoutingTable table = mockShardRoutingTable(primary, List.of(primary, replica));

      ShardSelector selector = new ShardSelector("node-3");
      ShardRouting selected = selector.select(table);

      assertThat(selected, sameInstance(replica));
    }

    @Test
    @DisplayName("returns null when no copy is available")
    void testReturnsNullWhenNoneAvailable() {
      ShardRouting primary = mockShardRouting("node-1", true, false);
      ShardRouting replica = mockShardRouting("node-2", false, false);

      IndexShardRoutingTable table = mockShardRoutingTable(primary, List.of(primary, replica));

      ShardSelector selector = new ShardSelector("node-3");
      ShardRouting selected = selector.select(table);

      assertNull(selected);
    }

    @Test
    @DisplayName("skips non-started replicas")
    void testSkipsNonStartedReplicas() {
      ShardRouting primary = mockShardRouting("node-1", true, false);
      ShardRouting stoppedReplica = mockShardRouting("node-2", false, false);
      ShardRouting startedReplica = mockShardRouting("node-3", false, true);

      IndexShardRoutingTable table =
          mockShardRoutingTable(primary, List.of(primary, stoppedReplica, startedReplica));

      ShardSelector selector = new ShardSelector("node-99");
      ShardRouting selected = selector.select(table);

      assertThat(selected, sameInstance(startedReplica));
    }

    private ShardRouting mockShardRouting(String nodeId, boolean primary, boolean started) {
      ShardRouting routing = mock(ShardRouting.class);
      when(routing.currentNodeId()).thenReturn(nodeId);
      when(routing.primary()).thenReturn(primary);
      when(routing.started()).thenReturn(started);
      return routing;
    }

    @SuppressWarnings("unchecked")
    private IndexShardRoutingTable mockShardRoutingTable(
        ShardRouting primary, List<ShardRouting> allRoutings) {
      IndexShardRoutingTable table = mock(IndexShardRoutingTable.class);
      when(table.primaryShard()).thenReturn(primary);
      Iterator<ShardRouting> iterator = allRoutings.iterator();
      when(table.iterator()).thenReturn(iterator);
      return table;
    }
  }

  // ---------------------------------------------------------------------------
  // M-5: SchemaSnapshot tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-5: SchemaSnapshot")
  class SchemaSnapshotTests {

    @Test
    @DisplayName("construction and getters")
    void testConstruction() {
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      List<DqeColumnHandle> columns =
          List.of(
              new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, null, false),
              new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false));

      SchemaSnapshot snapshot = new SchemaSnapshot(tableHandle, columns, 1000L);

      assertThat(snapshot.getTableHandle(), sameInstance(tableHandle));
      assertThat(snapshot.getColumns(), hasSize(2));
      assertEquals(1000L, snapshot.getCreatedAtMillis());
    }

    @Test
    @DisplayName("getColumn by fieldPath")
    void testGetColumnByPath() {
      DqeColumnHandle nameCol =
          new DqeColumnHandle("name", "user.name", DqeTypes.VARCHAR, true, null, false);
      DqeColumnHandle ageCol =
          new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      SchemaSnapshot snapshot = new SchemaSnapshot(tableHandle, List.of(nameCol, ageCol), 1000L);

      Optional<DqeColumnHandle> found = snapshot.getColumn("user.name");
      assertTrue(found.isPresent());
      assertEquals(nameCol, found.get());

      Optional<DqeColumnHandle> notFound = snapshot.getColumn("nonexistent");
      assertFalse(notFound.isPresent());
    }

    @Test
    @DisplayName("getColumn by ordinal")
    void testGetColumnByOrdinal() {
      DqeColumnHandle col0 = new DqeColumnHandle("a", "a", DqeTypes.VARCHAR, true, null, false);
      DqeColumnHandle col1 = new DqeColumnHandle("b", "b", DqeTypes.BIGINT, true, null, false);
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      SchemaSnapshot snapshot = new SchemaSnapshot(tableHandle, List.of(col0, col1), 1000L);

      assertEquals(col0, snapshot.getColumn(0));
      assertEquals(col1, snapshot.getColumn(1));
    }

    @Test
    @DisplayName("getColumnCount returns correct count")
    void testColumnCount() {
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      SchemaSnapshot snapshot = new SchemaSnapshot(tableHandle, List.of(), 1000L);
      assertEquals(0, snapshot.getColumnCount());

      DqeColumnHandle col = new DqeColumnHandle("x", "x", DqeTypes.DOUBLE, true, null, false);
      SchemaSnapshot snapshot2 = new SchemaSnapshot(tableHandle, List.of(col), 1000L);
      assertEquals(1, snapshot2.getColumnCount());
    }

    @Test
    @DisplayName("columns list is immutable")
    void testColumnsImmutable() {
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      DqeColumnHandle col = new DqeColumnHandle("x", "x", DqeTypes.DOUBLE, true, null, false);
      SchemaSnapshot snapshot = new SchemaSnapshot(tableHandle, List.of(col), 1000L);

      assertThrows(
          UnsupportedOperationException.class,
          () ->
              snapshot
                  .getColumns()
                  .add(new DqeColumnHandle("y", "y", DqeTypes.BIGINT, true, null, false)));
    }

    @Test
    @DisplayName("null tableHandle throws NPE")
    void testNullTableHandle() {
      assertThrows(NullPointerException.class, () -> new SchemaSnapshot(null, List.of(), 1000L));
    }

    @Test
    @DisplayName("null columns throws NPE")
    void testNullColumns() {
      DqeTableHandle tableHandle = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);
      assertThrows(NullPointerException.class, () -> new SchemaSnapshot(tableHandle, null, 1000L));
    }
  }

  // ---------------------------------------------------------------------------
  // M-6: DqeTableStatistics tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-6: DqeTableStatistics")
  class TableStatisticsTests {

    @Test
    @DisplayName("construction and getters")
    void testConstruction() {
      DqeTableStatistics stats = new DqeTableStatistics(100L, 4096L, 5L);
      assertEquals(100L, stats.getRowCount());
      assertEquals(4096L, stats.getIndexSizeBytes());
      assertEquals(5L, stats.getGeneration());
      assertFalse(stats.isUnknown());
    }

    @Test
    @DisplayName("UNKNOWN sentinel values")
    void testUnknownSentinel() {
      DqeTableStatistics unknown = DqeTableStatistics.UNKNOWN;
      assertEquals(-1L, unknown.getRowCount());
      assertEquals(-1L, unknown.getIndexSizeBytes());
      assertEquals(-1L, unknown.getGeneration());
      assertTrue(unknown.isUnknown());
    }

    @Test
    @DisplayName("isUnknown returns true for negative row count")
    void testIsUnknownForNegative() {
      DqeTableStatistics stats = new DqeTableStatistics(-5L, 100L, 1L);
      assertTrue(stats.isUnknown());
    }

    @Test
    @DisplayName("isUnknown returns false for zero row count")
    void testIsKnownForZero() {
      DqeTableStatistics stats = new DqeTableStatistics(0L, 0L, 1L);
      assertFalse(stats.isUnknown());
    }
  }

  // ---------------------------------------------------------------------------
  // M-6: StatisticsCache tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-6: StatisticsCache")
  class StatisticsCacheTests {

    @Test
    @DisplayName("put and get within TTL returns cached value")
    void testPutAndGet() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      DqeTableStatistics stats = new DqeTableStatistics(100L, 4096L, 5L);

      cache.put("my_index", stats);
      DqeTableStatistics retrieved = cache.get("my_index", 5L);

      assertEquals(100L, retrieved.getRowCount());
      assertEquals(4096L, retrieved.getIndexSizeBytes());
      assertFalse(retrieved.isUnknown());
    }

    @Test
    @DisplayName("get returns UNKNOWN for missing key")
    void testGetMissing() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      DqeTableStatistics result = cache.get("nonexistent", 1L);
      assertTrue(result.isUnknown());
    }

    @Test
    @DisplayName("get returns UNKNOWN for stale generation")
    void testStaleGeneration() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx", new DqeTableStatistics(100L, 4096L, 5L));

      DqeTableStatistics result = cache.get("idx", 6L); // different generation
      assertTrue(result.isUnknown());
    }

    @Test
    @DisplayName("invalidate removes specific entry")
    void testInvalidate() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx1", new DqeTableStatistics(10L, 100L, 1L));
      cache.put("idx2", new DqeTableStatistics(20L, 200L, 1L));

      cache.invalidate("idx1");

      assertTrue(cache.get("idx1", 1L).isUnknown());
      assertFalse(cache.get("idx2", 1L).isUnknown());
    }

    @Test
    @DisplayName("invalidateAll clears all entries")
    void testInvalidateAll() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx1", new DqeTableStatistics(10L, 100L, 1L));
      cache.put("idx2", new DqeTableStatistics(20L, 200L, 1L));

      cache.invalidateAll();

      assertTrue(cache.get("idx1", 1L).isUnknown());
      assertTrue(cache.get("idx2", 1L).isUnknown());
      assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("size reflects number of entries")
    void testSize() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      assertEquals(0, cache.size());

      cache.put("a", new DqeTableStatistics(1L, 1L, 1L));
      assertEquals(1, cache.size());

      cache.put("b", new DqeTableStatistics(2L, 2L, 1L));
      assertEquals(2, cache.size());
    }

    @Test
    @DisplayName("zero or negative TTL throws IllegalArgumentException")
    void testInvalidTtl() {
      assertThrows(IllegalArgumentException.class, () -> new StatisticsCache(0));
      assertThrows(IllegalArgumentException.class, () -> new StatisticsCache(-1));
    }

    @Test
    @DisplayName("overwrite existing entry with new stats")
    void testOverwrite() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx", new DqeTableStatistics(100L, 1000L, 1L));
      cache.put("idx", new DqeTableStatistics(200L, 2000L, 2L));

      DqeTableStatistics result = cache.get("idx", 2L);
      assertEquals(200L, result.getRowCount());
    }
  }

  // ---------------------------------------------------------------------------
  // Exception tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Exception classes")
  class ExceptionTests {

    @Test
    @DisplayName("DqeTableNotFoundException carries table name and error code")
    void testTableNotFound() {
      DqeTableNotFoundException ex = new DqeTableNotFoundException("missing_idx");
      assertEquals("missing_idx", ex.getTableName());
      assertEquals(DqeErrorCode.TABLE_NOT_FOUND, ex.getErrorCode());
      assertThat(ex.getMessage(), containsString("missing_idx"));
    }

    @Test
    @DisplayName("DqeShardNotAvailableException carries index name, shard ID, and error code")
    void testShardNotAvailable() {
      DqeShardNotAvailableException ex = new DqeShardNotAvailableException("my_idx", 3);
      assertEquals("my_idx", ex.getIndexName());
      assertEquals(3, ex.getShardId());
      assertEquals(DqeErrorCode.SHARD_NOT_AVAILABLE, ex.getErrorCode());
      assertThat(ex.getMessage(), containsString("my_idx"));
      assertThat(ex.getMessage(), containsString("3"));
    }
  }

  // ---------------------------------------------------------------------------
  // M-2: DqeMetadata.getTableHandle tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-2: DqeMetadata.getTableHandle")
  class GetTableHandleTests {

    @Test
    @DisplayName("resolves existing index to table handle")
    void testResolvesExistingIndex() {
      ClusterState clusterState = mockClusterStateWithIndex("test_index", 42L, null);
      DqeMetadata metadata = createMetadata();

      DqeTableHandle handle = metadata.getTableHandle(clusterState, "default", "test_index");

      assertEquals("test_index", handle.getIndexName());
      assertEquals(42L, handle.getSchemaGeneration());
      assertEquals(List.of("test_index"), handle.getResolvedIndices());
      assertNull(handle.getPitId());
    }

    @Test
    @DisplayName("throws DqeTableNotFoundException for missing index")
    void testThrowsForMissingIndex() {
      ClusterState clusterState = mockEmptyClusterState();
      DqeMetadata metadata = createMetadata();

      DqeTableNotFoundException ex =
          assertThrows(
              DqeTableNotFoundException.class,
              () -> metadata.getTableHandle(clusterState, "default", "nonexistent"));
      assertEquals("nonexistent", ex.getTableName());
    }

    @Test
    @DisplayName("schema generation reflects IndexMetadata version")
    void testSchemaGenerationFromVersion() {
      ClusterState clusterState = mockClusterStateWithIndex("idx", 99L, null);
      DqeMetadata metadata = createMetadata();

      DqeTableHandle handle = metadata.getTableHandle(clusterState, "default", "idx");
      assertEquals(99L, handle.getSchemaGeneration());
    }
  }

  // ---------------------------------------------------------------------------
  // M-3: DqeMetadata.getColumnHandles tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-3: DqeMetadata.getColumnHandles")
  class GetColumnHandlesTests {

    @Test
    @DisplayName("resolves columns from index mapping")
    void testResolvesColumns() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("name", Map.of("type", "keyword"));
      properties.put("age", Map.of("type", "integer"));
      properties.put("score", Map.of("type", "double"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);

      assertThat(columns, hasSize(3));
    }

    @Test
    @DisplayName("resolves keyword column as sortable VARCHAR")
    void testKeywordColumn() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("status", Map.of("type", "keyword"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);

      assertThat(columns, hasSize(1));
      DqeColumnHandle col = columns.get(0);
      assertEquals("status", col.getFieldName());
      assertEquals("status", col.getFieldPath());
      assertEquals(DqeTypes.VARCHAR, col.getType());
      assertTrue(col.isSortable());
    }

    @Test
    @DisplayName("resolves long column as BIGINT")
    void testLongColumn() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("count", Map.of("type", "long"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);
      assertEquals(DqeTypes.BIGINT, columns.get(0).getType());
    }

    @Test
    @DisplayName("resolves nested field paths correctly")
    void testNestedFieldPaths() {
      Map<String, Object> innerProps = new LinkedHashMap<>();
      innerProps.put("city", Map.of("type", "keyword"));

      Map<String, Object> addressMapping = new LinkedHashMap<>();
      addressMapping.put("type", "object");
      addressMapping.put("properties", innerProps);

      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("address", addressMapping);

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);

      // Should include both the object field and the nested field
      assertTrue(columns.stream().anyMatch(c -> c.getFieldPath().equals("address.city")));
    }

    @Test
    @DisplayName("returns empty list for index with no mapping")
    void testNoMapping() {
      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, null);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);
      assertTrue(columns.isEmpty());
    }

    @Test
    @DisplayName("throws DqeTableNotFoundException for missing index")
    void testThrowsForMissingIndex() {
      ClusterState clusterState = mockEmptyClusterState();
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("missing", null, List.of("missing"), 1L, null);

      assertThrows(
          DqeTableNotFoundException.class, () -> metadata.getColumnHandles(clusterState, table));
    }

    @Test
    @DisplayName("resolves date field as TIMESTAMP(3)")
    void testDateColumn() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("created_at", Map.of("type", "date"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);
      assertEquals(DqeTypes.TIMESTAMP_MILLIS, columns.get(0).getType());
    }

    @Test
    @DisplayName("resolves boolean field")
    void testBooleanColumn() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("active", Map.of("type", "boolean"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);
      assertEquals(DqeTypes.BOOLEAN, columns.get(0).getType());
    }

    @Test
    @DisplayName("leaf name extracted for nested field")
    void testLeafNameExtraction() {
      Map<String, Object> innerProps = new LinkedHashMap<>();
      innerProps.put("zip", Map.of("type", "keyword"));

      Map<String, Object> addressMapping = new LinkedHashMap<>();
      addressMapping.put("type", "object");
      addressMapping.put("properties", innerProps);

      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("address", addressMapping);

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeColumnHandle> columns = metadata.getColumnHandles(clusterState, table);
      Optional<DqeColumnHandle> zipCol =
          columns.stream().filter(c -> c.getFieldPath().equals("address.zip")).findFirst();
      assertTrue(zipCol.isPresent());
      assertEquals("zip", zipCol.get().getFieldName());
    }
  }

  // ---------------------------------------------------------------------------
  // M-4: DqeMetadata.getSplits tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-4: DqeMetadata.getSplits")
  class GetSplitsTests {

    @Test
    @DisplayName("produces one split per shard")
    void testOneSplitPerShard() {
      ClusterState clusterState = mockClusterStateWithShards("idx", 3);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeShardSplit> splits = metadata.getSplits(clusterState, table, "local-node");

      assertThat(splits, hasSize(3));
    }

    @Test
    @DisplayName("split contains correct shard IDs")
    void testShardIds() {
      ClusterState clusterState = mockClusterStateWithShards("idx", 2);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      List<DqeShardSplit> splits = metadata.getSplits(clusterState, table, "local-node");

      assertTrue(splits.stream().anyMatch(s -> s.getShardId() == 0));
      assertTrue(splits.stream().anyMatch(s -> s.getShardId() == 1));
    }

    @Test
    @DisplayName("split references correct index name")
    void testIndexName() {
      ClusterState clusterState = mockClusterStateWithShards("test_idx", 1);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("test_idx", null, List.of("test_idx"), 1L, null);

      List<DqeShardSplit> splits = metadata.getSplits(clusterState, table, "local-node");
      assertEquals("test_idx", splits.get(0).getIndexName());
    }

    @Test
    @DisplayName("throws DqeTableNotFoundException for missing routing table")
    void testMissingRoutingTable() {
      ClusterState clusterState = mockEmptyRoutingClusterState();
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("missing", null, List.of("missing"), 1L, null);

      assertThrows(
          DqeTableNotFoundException.class, () -> metadata.getSplits(clusterState, table, "node-1"));
    }

    @Test
    @DisplayName("throws DqeShardNotAvailableException when no copy available")
    void testShardNotAvailable() {
      ClusterState clusterState = mockClusterStateWithUnavailableShard("idx");
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      DqeShardNotAvailableException ex =
          assertThrows(
              DqeShardNotAvailableException.class,
              () -> metadata.getSplits(clusterState, table, "node-1"));
      assertEquals("idx", ex.getIndexName());
    }
  }

  // ---------------------------------------------------------------------------
  // M-6: DqeMetadata.getStatistics tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-6: DqeMetadata.getStatistics")
  class GetStatisticsTests {

    @Test
    @DisplayName("returns UNKNOWN when cache is empty")
    void testReturnsUnknownWhenEmpty() {
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      DqeTableStatistics stats = metadata.getStatistics(table);
      assertTrue(stats.isUnknown());
    }

    @Test
    @DisplayName("returns cached statistics when generation matches")
    void testReturnsCachedStats() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx", new DqeTableStatistics(500L, 8192L, 3L));

      DqeMetadata metadata = createMetadataWithCache(cache);
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 3L, null);

      DqeTableStatistics stats = metadata.getStatistics(table);
      assertFalse(stats.isUnknown());
      assertEquals(500L, stats.getRowCount());
    }

    @Test
    @DisplayName("returns UNKNOWN when generation differs")
    void testReturnsUnknownWhenGenerationDiffers() {
      StatisticsCache cache = new StatisticsCache(60_000L);
      cache.put("idx", new DqeTableStatistics(500L, 8192L, 3L));

      DqeMetadata metadata = createMetadataWithCache(cache);
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 4L, null);

      DqeTableStatistics stats = metadata.getStatistics(table);
      assertTrue(stats.isUnknown());
    }
  }

  // ---------------------------------------------------------------------------
  // M-5: DqeMetadata.createSnapshot tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("M-5: DqeMetadata.createSnapshot")
  class CreateSnapshotTests {

    @Test
    @DisplayName("creates snapshot with table handle and columns")
    void testCreatesSnapshot() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("name", Map.of("type", "keyword"));
      properties.put("value", Map.of("type", "long"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      SchemaSnapshot snapshot = metadata.createSnapshot(clusterState, table);

      assertThat(snapshot.getTableHandle(), equalTo(table));
      assertThat(snapshot.getColumns(), hasSize(2));
      assertTrue(snapshot.getCreatedAtMillis() > 0);
    }

    @Test
    @DisplayName("snapshot freezes columns at creation time")
    void testSnapshotFreezes() {
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("field1", Map.of("type", "keyword"));

      ClusterState clusterState = mockClusterStateWithIndex("idx", 1L, properties);
      DqeMetadata metadata = createMetadata();
      DqeTableHandle table = new DqeTableHandle("idx", null, List.of("idx"), 1L, null);

      SchemaSnapshot snapshot = metadata.createSnapshot(clusterState, table);

      // Columns are frozen
      assertThat(snapshot.getColumnCount(), is(1));
      assertEquals("field1", snapshot.getColumn(0).getFieldPath());
    }
  }

  // ---------------------------------------------------------------------------
  // Helper methods for mock setup
  // ---------------------------------------------------------------------------

  private DqeMetadata createMetadata() {
    return createMetadataWithCache(new StatisticsCache(60_000L));
  }

  private DqeMetadata createMetadataWithCache(StatisticsCache cache) {
    MultiFieldResolver multiFieldResolver = new MultiFieldResolver();
    DateFormatResolver dateFormatResolver = new DateFormatResolver();
    OpenSearchTypeMappingResolver typeMappingResolver =
        new OpenSearchTypeMappingResolver(multiFieldResolver, dateFormatResolver);
    return new DqeMetadata(typeMappingResolver, cache);
  }

  /**
   * Creates a mock ClusterState with one index that has the given properties mapping. If properties
   * is null, the index has no mapping.
   */
  private ClusterState mockClusterStateWithIndex(
      String indexName, long version, Map<String, Object> properties) {
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);

    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index(indexName)).thenReturn(indexMetadata);
    when(indexMetadata.getVersion()).thenReturn(version);

    if (properties != null) {
      MappingMetadata mappingMetadata = mock(MappingMetadata.class);
      when(indexMetadata.mapping()).thenReturn(mappingMetadata);
      Map<String, Object> sourceMap = Map.of("properties", properties);
      when(mappingMetadata.sourceAsMap()).thenReturn(sourceMap);
    } else {
      when(indexMetadata.mapping()).thenReturn(null);
    }

    return clusterState;
  }

  /** Creates a mock ClusterState with no indices. */
  private ClusterState mockEmptyClusterState() {
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
    return clusterState;
  }

  /**
   * Creates a mock ClusterState with a routing table containing N shards, all started primaries.
   */
  private ClusterState mockClusterStateWithShards(String indexName, int numShards) {
    ClusterState clusterState = mock(ClusterState.class);
    RoutingTable routingTable = mock(RoutingTable.class);
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

    when(clusterState.routingTable()).thenReturn(routingTable);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    Map<Integer, IndexShardRoutingTable> shardMap = new LinkedHashMap<>();
    for (int i = 0; i < numShards; i++) {
      IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
      ShardRouting primary = mock(ShardRouting.class);
      when(primary.primary()).thenReturn(true);
      when(primary.started()).thenReturn(true);
      when(primary.currentNodeId()).thenReturn("node-" + i);

      when(shardRoutingTable.primaryShard()).thenReturn(primary);
      when(shardRoutingTable.iterator()).thenReturn(List.of(primary).iterator());

      shardMap.put(i, shardRoutingTable);
    }

    when(indexRoutingTable.shards()).thenReturn(shardMap);

    return clusterState;
  }

  /** Creates a mock ClusterState with empty routing table. */
  private ClusterState mockEmptyRoutingClusterState() {
    ClusterState clusterState = mock(ClusterState.class);
    RoutingTable routingTable = mock(RoutingTable.class);
    when(clusterState.routingTable()).thenReturn(routingTable);
    when(routingTable.index(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
    return clusterState;
  }

  /** Creates a mock ClusterState where one shard has no available copies. */
  private ClusterState mockClusterStateWithUnavailableShard(String indexName) {
    ClusterState clusterState = mock(ClusterState.class);
    RoutingTable routingTable = mock(RoutingTable.class);
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

    when(clusterState.routingTable()).thenReturn(routingTable);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
    ShardRouting primary = mock(ShardRouting.class);
    when(primary.primary()).thenReturn(true);
    when(primary.started()).thenReturn(false); // not started
    when(primary.currentNodeId()).thenReturn("node-1");

    when(shardRoutingTable.primaryShard()).thenReturn(primary);
    when(shardRoutingTable.iterator()).thenReturn(List.of(primary).iterator());

    Map<Integer, IndexShardRoutingTable> shardMap = Map.of(0, shardRoutingTable);
    when(indexRoutingTable.shards()).thenReturn(shardMap);

    return clusterState;
  }
}
