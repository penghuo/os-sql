/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class AccessController {
  public static  <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return java.security.AccessController.doPrivileged(action);
    } catch (PrivilegedActionException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}
