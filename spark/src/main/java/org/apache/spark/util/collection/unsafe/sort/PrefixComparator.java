/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.annotation.Private;

/**
 * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific
 * comparisons, such as lexicographic comparison for strings.
 */
@Private
public abstract class PrefixComparator {
  public abstract int compare(long prefix1, long prefix2);
}
