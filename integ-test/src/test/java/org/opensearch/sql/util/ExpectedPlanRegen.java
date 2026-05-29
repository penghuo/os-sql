/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Bulk-regen helper for ExplainIT-style golden files. When the JVM property {@code
 * -Dregen.expected=true} is set, {@link MatcherUtils#assertYamlEqualsIgnoreId} (and similar
 * asserts) rewrite the source-tree expected file with the actual plan output instead of asserting
 * equality. The mechanism relies on {@link
 * org.opensearch.sql.ppl.PPLIntegTestCase#loadExpectedPlan(String)} stashing the resource path
 * via {@link #setLastResourcePath(String)} immediately before the assert call.
 *
 * <p>The path resolves to {@code integ-test/src/test/resources/<resource-path>}. With Gradle's
 * default test working dir at {@code integ-test/}, that's {@code src/test/resources/<resource-
 * path>} from {@code System.getProperty("user.dir")}. If the path is missing or the working dir
 * isn't {@code integ-test}, the helper logs a warning and lets the assert fail normally.
 *
 * <p>Workflow:
 *
 * <pre>{@code
 * # 1. Run the failing tests with regen on:
 * ./gradlew :integ-test:integTest -Dregen.expected=true -Dtests.class="*CalciteExplainIT"
 *
 * # 2. Inspect the diff:
 * git diff integ-test/src/test/resources/expectedOutput/calcite/
 *
 * # 3. Re-run without regen to confirm green:
 * ./gradlew :integ-test:integTest -Dtests.class="*CalciteExplainIT"
 * }</pre>
 */
public final class ExpectedPlanRegen {

  private ExpectedPlanRegen() {}

  private static final ThreadLocal<String> LAST_RESOURCE_PATH = new ThreadLocal<>();

  public static boolean isEnabled() {
    return Boolean.getBoolean("regen.expected");
  }

  public static void setLastResourcePath(String resourcePath) {
    LAST_RESOURCE_PATH.set(resourcePath);
  }

  public static String getLastResourcePath() {
    return LAST_RESOURCE_PATH.get();
  }

  /**
   * Write {@code content} to {@code integ-test/src/test/resources/<lastResourcePath>}. Returns
   * true if the write succeeded, false otherwise (no resource path stashed, the resolved path
   * doesn't look like a source tree, etc.).
   *
   * <p>Gradle's test JVM working dir is typically {@code build/testrun/<task>/}; the source-tree
   * root is computed by walking up to the {@code integ-test} directory and rooting at {@code
   * integ-test/src/test/resources}. The {@code project.root} system property (set by the
   * integTest task) is preferred as the most reliable anchor.
   */
  public static boolean writeToSourceTree(String content) {
    String resourcePath = LAST_RESOURCE_PATH.get();
    if (resourcePath == null) {
      System.err.println(
          "[regen.expected] no resource path stashed; ensure the test calls"
              + " loadExpectedPlan(...) before the assert.");
      return false;
    }
    Path projectRoot = resolveIntegTestRoot();
    if (projectRoot == null) {
      System.err.println(
          "[regen.expected] could not resolve integ-test source-tree root; refusing to write.");
      return false;
    }
    Path target = projectRoot.resolve("src/test/resources").resolve(resourcePath);
    try {
      Files.createDirectories(target.getParent());
      Files.write(
          target,
          content.getBytes(),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
      System.out.println("[regen.expected] wrote " + target);
      return true;
    } catch (IOException e) {
      System.err.println("[regen.expected] failed to write " + target + ": " + e.getMessage());
      return false;
    }
  }

  private static Path resolveIntegTestRoot() {
    // 1. project.root system property is set by integ-test/build.gradle to project.projectDir.
    String projectRoot = System.getProperty("project.root");
    if (projectRoot != null) {
      Path p = Paths.get(projectRoot);
      if (p.endsWith("integ-test")) {
        return p;
      }
    }
    // 2. Fallback — walk up from user.dir until we find a parent named integ-test.
    Path cur = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    while (cur != null) {
      if (cur.getFileName() != null && cur.getFileName().toString().equals("integ-test")) {
        return cur;
      }
      cur = cur.getParent();
    }
    return null;
  }
}
