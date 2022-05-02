/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * URL class loader that exposes the `addURL` method in URLClassLoader.
 */
public class MutableURLClassLoader extends URLClassLoader {

  static {
    ClassLoader.registerAsParallelCapable();
  }

  public MutableURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  public void addURL(URL url) {
    super.addURL(url);
  }
}
