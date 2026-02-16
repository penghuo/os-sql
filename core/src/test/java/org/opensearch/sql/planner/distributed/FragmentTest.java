/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FragmentTest {

  @Test
  @DisplayName("source() should create a SOURCE fragment with splits and no children")
  void sourceFactoryMethod() {
    List<ShardSplit> splits =
        Arrays.asList(
            new ShardSplit("idx", 0, "node1", Collections.emptyList(), true),
            new ShardSplit("idx", 1, "node2", Collections.emptyList(), false));

    Fragment fragment = Fragment.source(0, splits, 1000L);

    assertEquals(0, fragment.getFragmentId());
    assertEquals(FragmentType.SOURCE, fragment.getType());
    assertTrue(fragment.getChildren().isEmpty());
    assertNull(fragment.getExchangeSpec());
    assertEquals(splits, fragment.getSplits());
    assertEquals(1000L, fragment.getEstimatedRows());
  }

  @Test
  @DisplayName("single() should create a SINGLE fragment with children and exchange spec")
  void singleFactoryMethod() {
    Fragment sourceFragment =
        Fragment.source(
            0,
            Collections.singletonList(
                new ShardSplit("idx", 0, "node1", Collections.emptyList(), true)),
            500L);

    ExchangeSpec exchangeSpec = ExchangeSpec.gather();
    Fragment singleFragment =
        Fragment.single(1, Collections.singletonList(sourceFragment), exchangeSpec, 100L);

    assertEquals(1, singleFragment.getFragmentId());
    assertEquals(FragmentType.SINGLE, singleFragment.getType());
    assertEquals(1, singleFragment.getChildren().size());
    assertEquals(sourceFragment, singleFragment.getChildren().get(0));
    assertEquals(exchangeSpec, singleFragment.getExchangeSpec());
    assertTrue(singleFragment.getSplits().isEmpty());
    assertEquals(100L, singleFragment.getEstimatedRows());
  }

  @Test
  @DisplayName("hash() should create a HASH fragment with children and hash exchange")
  void hashFactoryMethod() {
    Fragment leftSource =
        Fragment.source(
            0,
            Collections.singletonList(
                new ShardSplit("left_idx", 0, "node1", Collections.emptyList(), true)),
            200L);
    Fragment rightSource =
        Fragment.source(
            1,
            Collections.singletonList(
                new ShardSplit("right_idx", 0, "node2", Collections.emptyList(), false)),
            300L);

    ExchangeSpec hashExchange = ExchangeSpec.hash(Collections.singletonList("join_key"));
    Fragment hashFragment = Fragment.hash(2, Arrays.asList(leftSource, rightSource), hashExchange, 150L);

    assertEquals(2, hashFragment.getFragmentId());
    assertEquals(FragmentType.HASH, hashFragment.getType());
    assertEquals(2, hashFragment.getChildren().size());
    assertEquals(hashExchange, hashFragment.getExchangeSpec());
    assertTrue(hashFragment.getSplits().isEmpty());
    assertEquals(150L, hashFragment.getEstimatedRows());
  }

  @Test
  @DisplayName("mutableChildren() should return a modifiable copy")
  void mutableChildrenReturnsCopy() {
    Fragment source =
        Fragment.source(
            0,
            Collections.singletonList(
                new ShardSplit("idx", 0, "n1", Collections.emptyList(), true)),
            100L);
    Fragment single =
        Fragment.single(1, Collections.singletonList(source), ExchangeSpec.gather(), 50L);

    List<Fragment> mutable = single.mutableChildren();
    assertEquals(1, mutable.size());

    // Adding to the mutable copy should not affect the original
    mutable.add(
        Fragment.source(2, Collections.emptyList(), 0L));
    assertEquals(1, single.getChildren().size());
    assertEquals(2, mutable.size());
  }

  @Test
  @DisplayName("Fragment should support nested children (multi-level plan)")
  void nestedFragments() {
    Fragment leafSource =
        Fragment.source(
            0,
            Collections.singletonList(
                new ShardSplit("data", 0, "node1", Collections.emptyList(), true)),
            10000L);

    Fragment midFragment =
        Fragment.single(
            1, Collections.singletonList(leafSource), ExchangeSpec.gather(), 500L);

    Fragment topFragment =
        Fragment.single(
            2,
            Collections.singletonList(midFragment),
            ExchangeSpec.orderedGather(Collections.singletonList("ts")),
            100L);

    assertEquals(FragmentType.SINGLE, topFragment.getType());
    assertEquals(1, topFragment.getChildren().size());
    Fragment child = topFragment.getChildren().get(0);
    assertEquals(FragmentType.SINGLE, child.getType());
    assertEquals(1, child.getChildren().size());
    assertEquals(FragmentType.SOURCE, child.getChildren().get(0).getType());
  }

  @Test
  @DisplayName("equals and hashCode should work correctly")
  void equalsAndHashCode() {
    List<ShardSplit> splits =
        Collections.singletonList(
            new ShardSplit("idx", 0, "node1", Collections.emptyList(), true));
    Fragment f1 = Fragment.source(0, splits, 100L);
    Fragment f2 = Fragment.source(0, splits, 100L);
    assertEquals(f1, f2);
    assertEquals(f1.hashCode(), f2.hashCode());
  }
}
