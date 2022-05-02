/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util;

/**
 * A class loader which makes some protected methods in ClassLoader accessible.
 */
public class ParentClassLoader extends ClassLoader {

  static {
    ClassLoader.registerAsParallelCapable();
  }

  public ParentClassLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    return super.findClass(name);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    return super.loadClass(name, resolve);
  }
}
