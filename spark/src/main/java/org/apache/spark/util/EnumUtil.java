/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.spark.util;

import com.google.common.base.Joiner;
import org.apache.spark.annotation.Private;

@Private
public class EnumUtil {
  public static <E extends Enum<E>> E parseIgnoreCase(Class<E> clz, String str) {
    E[] constants = clz.getEnumConstants();
    if (str == null) {
      return null;
    }
    for (E e : constants) {
      if (e.name().equalsIgnoreCase(str)) {
        return e;
      }
    }
    throw new IllegalArgumentException(
      String.format("Illegal type='%s'. Supported type values: %s",
        str, Joiner.on(", ").join(constants)));
  }
}
