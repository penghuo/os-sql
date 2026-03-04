/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("PageBuilder: convert row maps to Trino Pages")
class PageBuilderTest {

  @Test
  @DisplayName("Build a page from two rows with varchar and bigint columns")
  void buildPageFromRows() {
    List<ColumnHandle> columns =
        List.of(
            new ColumnHandle("name", VarcharType.VARCHAR),
            new ColumnHandle("age", BigintType.BIGINT));

    List<Map<String, Object>> rows =
        List.of(Map.of("name", "alice", "age", 30), Map.of("name", "bob", "age", 25));

    Page page = PageBuilder.build(columns, rows);

    assertEquals(2, page.getPositionCount());
    assertEquals(2, page.getChannelCount());
    assertEquals(30L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
    assertEquals(25L, BigintType.BIGINT.getLong(page.getBlock(1), 1));
  }

  @Test
  @DisplayName("Null values produce null entries in blocks")
  void nullValuesHandled() {
    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    List<Map<String, Object>> rows = List.of(Map.of(), Map.of("val", 42));

    Page page = PageBuilder.build(columns, rows);

    assertEquals(2, page.getPositionCount());
    assertEquals(true, page.getBlock(0).isNull(0));
    assertEquals(42L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }
}
