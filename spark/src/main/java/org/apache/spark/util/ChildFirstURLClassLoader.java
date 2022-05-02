/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

/**
 * A mutable class loader that gives preference to its own URLs over the parent class loader
 * when loading classes and resources.
 */
public class ChildFirstURLClassLoader extends MutableURLClassLoader {

  static {
    ClassLoader.registerAsParallelCapable();
  }

  private ParentClassLoader parent;

  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, null);
    this.parent = new ParentClassLoader(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      return super.loadClass(name, resolve);
    } catch (ClassNotFoundException cnf) {
      return parent.loadClass(name, resolve);
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    ArrayList<URL> urls = Collections.list(super.getResources(name));
    urls.addAll(Collections.list(parent.getResources(name)));
    return Collections.enumeration(urls);
  }

  @Override
  public URL getResource(String name) {
    URL url = super.getResource(name);
    if (url != null) {
      return url;
    } else {
      return parent.getResource(name);
    }
  }
}
