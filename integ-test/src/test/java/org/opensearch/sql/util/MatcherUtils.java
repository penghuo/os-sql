/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.gson.JsonParser;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.utils.YamlFormatter;

public class MatcherUtils {

  /** Absolute tolerance floor for {@link #closeTo} numeric comparisons. */
  private static final double ABSOLUTE_TOLERANCE = 1e-10;

  /** Number of ULPs tolerated by {@link #closeTo} to absorb platform-dependent rounding. */
  private static final int ULP_TOLERANCE_FACTOR = 4;

  private static final Logger LOG = LogManager.getLogger();
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /**
   * Assert field value in object by a custom matcher and getter to access the field.
   *
   * @param name description
   * @param subMatcher sub-matcher for field
   * @param getter getter function to access the field
   * @param <T> type of outer object
   * @param <U> type of inner field
   * @return matcher
   */
  public static <T, U> FeatureMatcher<T, U> featureValueOf(
      String name, Matcher<U> subMatcher, Function<T, U> getter) {
    return new FeatureMatcher<T, U>(subMatcher, name, name) {
      @Override
      protected U featureValueOf(T actual) {
        return getter.apply(actual);
      }
    };
  }

  @SafeVarargs
  public static Matcher<SearchHits> hits(Matcher<SearchHit>... hitMatchers) {
    if (hitMatchers.length == 0) {
      return featureValueOf("SearchHits", emptyArray(), SearchHits::getHits);
    }
    return featureValueOf(
        "SearchHits", arrayContainingInAnyOrder(hitMatchers), SearchHits::getHits);
  }

  @SafeVarargs
  public static Matcher<SearchHits> hitsInOrder(Matcher<SearchHit>... hitMatchers) {
    if (hitMatchers.length == 0) {
      return featureValueOf("SearchHits", emptyArray(), SearchHits::getHits);
    }
    return featureValueOf("SearchHits", arrayContaining(hitMatchers), SearchHits::getHits);
  }

  @SuppressWarnings("unchecked")
  public static Matcher<SearchHit> hit(Matcher<Map<String, Object>>... entryMatchers) {
    return featureValueOf("SearchHit", allOf(entryMatchers), SearchHit::getSourceAsMap);
  }

  @SuppressWarnings("unchecked")
  public static Matcher<Map<String, Object>> kv(String key, Object value) {
    // Use raw type to avoid generic type problem from Matcher<Map<K,V>> to Matcher<String,Object>
    return (Matcher) hasEntry(key, value);
  }

  public static Matcher<JSONObject> hitAny(String query, Matcher<JSONObject>... matcher) {
    return featureValueOf(
        "SearchHits",
        hasItems(matcher),
        actual -> {
          JSONArray array = (JSONArray) (actual.query(query));
          List<JSONObject> results = new ArrayList<>(array.length());
          for (Object element : array) {
            results.add((JSONObject) element);
          }
          return results;
        });
  }

  public static Matcher<JSONObject> hitAny(Matcher<JSONObject>... matcher) {
    return hitAny("/hits/hits", matcher);
  }

  public static Matcher<JSONObject> hitAll(Matcher<JSONObject>... matcher) {
    return featureValueOf(
        "SearchHits",
        containsInAnyOrder(matcher),
        actual -> {
          JSONArray array = (JSONArray) (actual.query("/hits/hits"));
          List<JSONObject> results = new ArrayList<>(array.length());
          for (Object element : array) {
            results.add((JSONObject) element);
          }
          return results;
        });
  }

  public static Matcher<JSONObject> kvString(String key, Matcher<String> matcher) {
    return featureValueOf("Json Match", matcher, actual -> (String) actual.query(key));
  }

  public static Matcher<JSONObject> kvDouble(String key, Matcher<Double> matcher) {
    return featureValueOf(
        "Json Match", matcher, actual -> ((BigDecimal) actual.query(key)).doubleValue());
  }

  public static Matcher<JSONObject> kvInt(String key, Matcher<Integer> matcher) {
    return featureValueOf("Json Match", matcher, actual -> (Integer) actual.query(key));
  }

  @SafeVarargs
  public static void verifySchema(JSONObject response, Matcher<JSONObject>... matchers) {
    try {
      verify(response.getJSONArray("schema"), matchers);
    } catch (Exception e) {
      LOG.error(String.format("verify schema failed, response: %s", response.toString()), e);
      throw e;
    }
  }

  @SafeVarargs
  public static void verifySchemaInOrder(JSONObject response, Matcher<JSONObject>... matchers) {
    try {
      verifyInOrder(response.getJSONArray("schema"), matchers);
    } catch (Exception e) {
      LOG.error(String.format("verify schema failed, response: %s", response.toString()), e);
      throw e;
    }
  }

  @SafeVarargs
  public static void verifyDataRows(JSONObject response, Matcher<JSONArray>... matchers) {
    verify(response.getJSONArray("datarows"), matchers);
  }

  @SafeVarargs
  public static void verifyDataAddressRows(JSONObject response, Matcher<JSONArray>... matchers) {
    verifyAddressRow(response.getJSONArray("datarows"), matchers);
  }

  @SafeVarargs
  public static void verifyColumn(JSONObject response, Matcher<JSONObject>... matchers) {
    verify(response.getJSONArray("schema"), matchers);
  }

  @SafeVarargs
  public static void verifyOrder(JSONObject response, Matcher<JSONArray>... matchers) {
    verifyOrder(response.getJSONArray("datarows"), matchers);
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  public static void verifyDataRowsInOrder(JSONObject response, Matcher<JSONArray>... matchers) {
    verifyInOrder(response.getJSONArray("datarows"), matchers);
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  public static void verifyDataRowsSome(JSONObject response, Matcher<JSONArray>... matchers) {
    verifySome(response.getJSONArray("datarows"), matchers);
  }

  public static void verifyNumOfRows(JSONObject response, int numOfRow) {
    assertEquals(numOfRow, response.getJSONArray("datarows").length());
  }

  @SuppressWarnings("unchecked")
  public static <T> void verify(JSONArray array, Matcher<T>... matchers) {
    List<T> objects = new ArrayList<>();
    array.iterator().forEachRemaining(o -> objects.add((T) o));
    assertEquals(matchers.length, objects.size());
    assertThat(objects, containsInAnyOrder(matchers));
  }

  // TODO: this is temporary fix for fixing serverless tests to pass as it creates multiple shards
  // leading to score differences.
  public static <T> void verifyAddressRow(JSONArray array, Matcher<T>... matchers) {
    // List to store the processed elements from the JSONArray
    List<T> objects = new ArrayList<>();

    // Iterate through each element in the JSONArray
    array
        .iterator()
        .forEachRemaining(
            o -> {
              // Check if o is a JSONArray with exactly 2 elements
              if (o instanceof JSONArray && ((JSONArray) o).length() == 2) {
                // Check if the second element is a BigDecimal/_score value
                if (((JSONArray) o).get(1) instanceof BigDecimal) {
                  // Remove the _score element from response data rows to skip the assertion as it
                  // will be different when compared against multiple shards
                  ((JSONArray) o).remove(1);
                }
              }
              objects.add((T) o);
            });
    assertEquals(matchers.length, objects.size());
    assertThat(objects, containsInAnyOrder(matchers));
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  public static <T> void verifyInOrder(JSONArray array, Matcher<T>... matchers) {
    List<T> objects = new ArrayList<>();
    array.iterator().forEachRemaining(o -> objects.add((T) o));
    assertEquals(matchers.length, objects.size());
    assertThat(objects, contains(matchers));
  }

  @SuppressWarnings("unchecked")
  public static <T> void verifySome(JSONArray array, Matcher<T>... matchers) {
    List<T> objects = new ArrayList<>();
    array.iterator().forEachRemaining(o -> objects.add((T) o));

    assertThat(matchers.length, greaterThan(0));
    for (Matcher<T> matcher : matchers) {
      assertThat(objects, hasItems(matcher));
    }
  }

  @SafeVarargs
  public static <T> void verifyOrder(JSONArray array, Matcher<T>... matchers) {
    List<T> objects = new ArrayList<>();
    array.iterator().forEachRemaining(o -> objects.add((T) o));
    assertEquals(matchers.length, objects.size());
    assertThat(objects, containsInRelativeOrder(matchers));
  }

  public static void verifyErrorMessageContains(Throwable t, String msg) {
    String stack = ExceptionUtils.getStackTrace(t);
    assertThat(String.format("Actual stack trace was:\n%s", stack), stack.contains(msg));
  }

  public static TypeSafeMatcher<JSONObject> schema(String expectedName, String expectedType) {
    return schema(expectedName, null, expectedType);
  }

  public static TypeSafeMatcher<JSONObject> schema(
      String expectedName, String expectedAlias, String expectedType) {
    return new TypeSafeMatcher<JSONObject>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format(
                "(name=%s, alias=%s, type=%s)", expectedName, expectedAlias, expectedType));
      }

      @Override
      protected boolean matchesSafely(JSONObject jsonObject) {
        String actualName = (String) jsonObject.query("/name");
        String actualAlias = (String) jsonObject.query("/alias");
        String actualType = (String) jsonObject.query("/type");
        return expectedName.equals(actualName)
            && (Strings.isNullOrEmpty(expectedAlias) || expectedAlias.equals(actualAlias))
            && expectedType.equals(actualType);
      }
    };
  }

  public static TypeSafeMatcher<JSONArray> rows(Object... expectedObjects) {
    return new TypeSafeMatcher<JSONArray>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.join(",", Arrays.asList(expectedObjects).toString()));
      }

      @Override
      protected boolean matchesSafely(JSONArray array) {
        if (array.similar(new JSONArray(expectedObjects))) {
          return true;
        }
        return jsonAwareSimilar(array, new JSONArray(expectedObjects));
      }
    };
  }

  /**
   * JSON-aware variant of {@link JSONArray#similar(Object)} that also treats a JSON-encoded
   * String as similar to its parsed JSONObject / JSONArray counterpart. Calcite-engine PPL
   * functions like {@code json(x)} and {@code cast(x as json)} project the result as a
   * VARCHAR column, while the v2 engine surfaces it as a parsed JSON object/array. The
   * underlying content is identical; this comparator absorbs the type difference so the same
   * test fixtures pass on both engines.
   */
  private static boolean jsonAwareSimilar(JSONArray actual, JSONArray expected) {
    if (actual.length() != expected.length()) {
      return false;
    }
    for (int i = 0; i < actual.length(); i++) {
      Object a = actual.opt(i);
      Object e = expected.opt(i);
      if (!jsonAwareValueSimilar(a, e)) {
        return false;
      }
    }
    return true;
  }

  private static boolean jsonAwareValueSimilar(Object a, Object e) {
    if (a == null || e == null || a == JSONObject.NULL || e == JSONObject.NULL) {
      return (a == null || a == JSONObject.NULL) && (e == null || e == JSONObject.NULL);
    }
    if (a instanceof JSONObject ao && e instanceof JSONObject eo) {
      return ao.similar(eo);
    }
    if (a instanceof JSONArray aa && e instanceof JSONArray ea) {
      return jsonAwareSimilar(aa, ea);
    }
    if (a instanceof String sa && e instanceof JSONObject eo) {
      try {
        return new JSONObject(sa).similar(eo);
      } catch (Exception ex) {
        return false;
      }
    }
    if (a instanceof String sa && e instanceof JSONArray ea) {
      try {
        return jsonAwareSimilar(new JSONArray(sa), ea);
      } catch (Exception ex) {
        return false;
      }
    }
    // Both are Strings. They may be JSON-encoded; if both parse as JSON with equivalent
    // content (different field order, same keys/values), accept that as similar. The Calcite
    // engine's `json(x)` and `cast(x as json)` produce a Jackson-ordered canonical form
    // while the test fixture pins Gson-ordered insertion (Map.of(...)). Same content.
    if (a instanceof String sa && e instanceof String se && !sa.equals(se)) {
      String saTrim = sa.trim();
      String seTrim = se.trim();
      if ((saTrim.startsWith("{") && seTrim.startsWith("{"))
          || (saTrim.startsWith("[") && seTrim.startsWith("["))) {
        try {
          if (saTrim.startsWith("{")) {
            return new JSONObject(sa).similar(new JSONObject(se));
          } else {
            return jsonAwareSimilar(new JSONArray(sa), new JSONArray(se));
          }
        } catch (Exception ex) {
          // fall through to equals check
        }
      }
    }
    if (a instanceof Number an && e instanceof Number en) {
      return an.doubleValue() == en.doubleValue();
    }
    // Calcite's `json(x)` / `cast(x as json)` returns scalars as String (column is VARCHAR);
    // compare the textual form against any expected primitive (Number/Boolean) by parsing.
    if (a instanceof String sa && e instanceof Number en) {
      try {
        double ad = Double.parseDouble(sa);
        double ed = en.doubleValue();
        if (ad == ed) return true;
        // float/double round-trip tolerance: when the test fixture pins 12.34f (Float) and
        // the actual is "12.34" parsed as double, the bit-exact representations differ by
        // ~1.5e-7 (the float→double precision gap). Compare relative to the larger magnitude
        // to absorb both float-precision and runtime-decimal round trips.
        double mag = Math.max(Math.abs(ad), Math.abs(ed));
        double eps = (mag == 0 ? 1e-9 : mag * 1e-6);
        return Math.abs(ad - ed) <= eps;
      } catch (NumberFormatException ex) {
        return false;
      }
    }
    if (a instanceof String sa && e instanceof Boolean eb) {
      return Boolean.parseBoolean(sa) == eb;
    }
    return java.util.Objects.equals(a, e);
  }

  public static TypeSafeMatcher<JSONArray> closeTo(Object... values) {
    return new TypeSafeMatcher<JSONArray>() {
      @Override
      protected boolean matchesSafely(JSONArray item) {
        List<Object> expectedValues = new ArrayList<>(Arrays.asList(values));
        List<Object> actualValues = new ArrayList<>();
        item.iterator().forEachRemaining(v -> actualValues.add((Object) v));
        if (actualValues.size() != expectedValues.size()) {
          return false;
        }
        for (int i = 0; i < actualValues.size(); i++) {
          Object actual = actualValues.get(i);
          Object expected = expectedValues.get(i);
          if (actual instanceof Number && expected instanceof Number) {
            if (!valuesAreClose((Number) actual, (Number) expected)) {
              return false;
            }
          } else if (!actual.equals(expected)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(Arrays.toString(values));
      }

      /**
       * ULP-aware comparison: tolerates up to {@link #ULP_TOLERANCE_FACTOR} ULPs or {@link
       * #ABSOLUTE_TOLERANCE}, whichever is larger.
       */
      private boolean valuesAreClose(Number v1, Number v2) {
        double d1 = v1.doubleValue();
        double d2 = v2.doubleValue();
        double diff = Math.abs(d1 - d2);
        double ulpTolerance = ULP_TOLERANCE_FACTOR * Math.max(Math.ulp(d1), Math.ulp(d2));
        return diff <= Math.max(ABSOLUTE_TOLERANCE, ulpTolerance);
      }
    };
  }

  public static TypeSafeMatcher<JSONObject> columnPattern(String regex) {
    return new TypeSafeMatcher<JSONObject>() {
      @Override
      protected boolean matchesSafely(JSONObject jsonObject) {
        return ((String) jsonObject.query("/name")).matches(regex);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("(column_pattern=%s)", regex));
      }
    };
  }

  public static TypeSafeMatcher<JSONObject> columnName(String name) {
    return new TypeSafeMatcher<JSONObject>() {
      @Override
      protected boolean matchesSafely(JSONObject jsonObject) {
        return jsonObject.query("/name").equals(name);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("(name=%s)", name));
      }
    };
  }

  /** Tests if a string is equal to another string, ignore the case and whitespace. */
  public static class IsEqualIgnoreCaseAndWhiteSpace extends TypeSafeMatcher<String> {
    private final String string;

    public IsEqualIgnoreCaseAndWhiteSpace(String string) {
      if (string == null) {
        throw new IllegalArgumentException("Non-null value required");
      }
      this.string = string;
    }

    @Override
    public boolean matchesSafely(String item) {
      return ignoreCase(ignoreSpaces(string)).equals(ignoreCase(ignoreSpaces(item)));
    }

    @Override
    public void describeMismatchSafely(String item, Description mismatchDescription) {
      mismatchDescription.appendText("was ").appendValue(item);
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a string equal to ")
          .appendValue(string)
          .appendText(" ignore case and white space");
    }

    public String ignoreSpaces(String toBeStripped) {
      return toBeStripped.replaceAll("\\s+", "").trim();
    }

    public String ignoreCase(String toBeLower) {
      return toBeLower.toLowerCase();
    }

    public static Matcher<String> equalToIgnoreCaseAndWhiteSpace(String expectedString) {
      return new IsEqualIgnoreCaseAndWhiteSpace(expectedString);
    }
  }

  /**
   * Compare two JSON string are equals.
   *
   * @param expected expected JSON string.
   * @param actual actual JSON string.
   */
  public static void assertJsonEquals(String expected, String actual) {
    if (ExpectedPlanRegen.isEnabled() && ExpectedPlanRegen.writeToSourceTree(actual)) {
      return;
    }
    assertEquals(
        JsonParser.parseString(eliminatePid(expected)),
        JsonParser.parseString(eliminatePid(actual)));
  }

  /**
   * Compare two JSON string are equals with ignoring the RelNode id in the Calcite plan.
   * Deprecated, use {@link #assertYamlEqualsIgnoreId(String, String)}
   */
  @Deprecated
  public static void assertJsonEqualsIgnoreId(String expected, String actual) {
    if (ExpectedPlanRegen.isEnabled() && ExpectedPlanRegen.writeToSourceTree(actual)) {
      return;
    }
    assertJsonEquals(cleanUpId(expected), cleanUpId(actual));
  }

  private static String cleanUpId(String s) {
    return eliminateTimeStamp(eliminatePid(eliminateRelId(eliminateRequestOptions(s))));
  }

  private static String eliminateTimeStamp(String s) {
    return s.replaceAll("\\\\\"utcTimestamp\\\\\":\\d+", "\\\\\"utcTimestamp\\\\\":*");
  }

  private static String eliminateRelId(String s) {
    return s.replaceAll("rel#\\d+", "rel#")
        .replaceAll("RelSubset#\\d+", "RelSubset#")
        .replaceAll("LogicalProject#\\d+", "LogicalProject#")
        .replaceAll("id = \\d+", "id = *");
  }

  private static String eliminateRequestOptions(String s) {
    return s.replaceAll(" needClean=true,", "").replaceAll(" searchDone=false,", "");
  }

  private static String eliminatePid(String s) {
    return s.replaceAll("pitId=[^,]+,", "pitId=*,");
  }

  /** Compare two YAML strings are equals with ignoring the RelNode id in the Calcite plan. */
  public static void assertYamlEqualsIgnoreId(String expectedYaml, String actualYaml) {
    if (ExpectedPlanRegen.isEnabled() && ExpectedPlanRegen.writeToSourceTree(actualYaml)) {
      return;
    }
    assertYamlEquals(cleanUpYaml(expectedYaml), cleanUpYaml(actualYaml));
  }

  /**
   * Compare actual YAML with two expected YAML strings, using the second as a fallback. This is
   * useful when the DSL implementation can produce multiple valid plan variants. If the first
   * comparison fails, attempts the second comparison instead.
   *
   * @param expectedYaml1 the primary expected YAML string
   * @param expectedYaml2 the fallback expected YAML string
   * @param actualYaml the actual YAML string to compare
   * @throws AssertionError if both comparisons fail (reports only the second failure)
   */
  public static void assertYamlEqualsIgnoreId(
      String expectedYaml1, String expectedYaml2, String actualYaml) {
    // Dual-expected (primary + alternative) variants are used to tolerate non-deterministic map
    // ordering in the dumped plan output (e.g. script_fields HashMap iteration). Skip regen
    // mode here so the manually-curated pair survives — overwriting one file with the actual
    // collapses both branches and breaks the alternate-order run.
    if (ExpectedPlanRegen.isEnabled()) {
      // Try primary first, fall back to alternative; if both fail, write back to the primary
      // (the LAST-loaded resource path) so a regen still has effect when the diff is real.
      try {
        assertYamlEquals(cleanUpYaml(expectedYaml1), cleanUpYaml(actualYaml));
        return;
      } catch (AssertionError e1) {
        try {
          assertYamlEquals(cleanUpYaml(expectedYaml2), cleanUpYaml(actualYaml));
          return;
        } catch (AssertionError e2) {
          // Both failed — write actual back to primary so a real plan-shape change is captured.
          // The alternative file is preserved; manual review may be needed.
          ExpectedPlanRegen.writeToSourceTree(actualYaml);
          return;
        }
      }
    }
    try {
      assertYamlEquals(cleanUpYaml(expectedYaml1), cleanUpYaml(actualYaml));
    } catch (AssertionError e) {
      assertYamlEquals(cleanUpYaml(expectedYaml2), cleanUpYaml(actualYaml));
    }
  }

  public static void assertYamlEquals(String expected, String actual) {
    String normalizedExpected = normalizeLineBreaks(expected).trim();
    String normalizedActual = normalizeLineBreaks(actual).trim();
    assertEquals(
        formatMessage(normalizedExpected, normalizedActual), normalizedExpected, normalizedActual);
  }

  private static String normalizeLineBreaks(String s) {
    return s.replace("\r\n", "\n").replace("\r", "\n");
  }

  private static String cleanUpYaml(String s) {
    return sortScriptFieldsByKey(
        s.replaceAll("\"utcTimestamp\":\\d+", "\"utcTimestamp\": 0")
            .replaceAll("rel#\\d+", "rel#")
            .replaceAll("RelSubset#\\d+", "RelSubset#")
            .replaceAll("LogicalProject#\\d+", "LogicalProject#")
            .replaceAll("pitId=[^,]+,", "pitId=*,")
            .replaceAll(" needClean=true,", "")
            .replaceAll(" searchDone=false,", "")
            .replaceAll("id = \\d+", "id = *")
            // Calcite names synthetic Project columns `$fN` where N is the position of the
            // first synthetic field in the SOURCE row-type before projection trimming —
            // so the same plan can produce `$f1` or `$f90` depending on whether the trim
            // happens before or after Calcite assigns names. The expressions inside the
            // brackets are the meaningful identity; collapse the `$fN` label to `$f*` so
            // both forms compare equal.
            .replaceAll("\\$f\\d+=", "\\$f*="));
  }

  /**
   * Normalize {@code "script_fields":{...}} JSON sub-objects so their top-level keys appear in
   * alphabetical order. The OpenSearch {@code TopHitsAggregationBuilder} stores scriptFields in a
   * {@link java.util.HashSet}, so its serialized iteration order is unspecified — explain-plan
   * tests would otherwise be flaky run-to-run. This rewrites every occurrence in place so two
   * runs produce a comparable string.
   */
  private static String sortScriptFieldsByKey(String s) {
    String marker = "\"script_fields\":{";
    StringBuilder out = new StringBuilder(s.length());
    int i = 0;
    while (i < s.length()) {
      int idx = s.indexOf(marker, i);
      if (idx < 0) {
        out.append(s, i, s.length());
        break;
      }
      out.append(s, i, idx).append(marker);
      int contentStart = idx + marker.length();
      int objEnd = matchingBraceEnd(s, contentStart - 1); // points at '{'
      if (objEnd < 0) {
        // Unbalanced — give up and emit verbatim from this point.
        out.append(s, contentStart, s.length());
        break;
      }
      String body = s.substring(contentStart, objEnd);
      String sorted = sortJsonObjectBodyByKey(body);
      out.append(sorted).append('}');
      i = objEnd + 1;
    }
    return out.toString();
  }

  /** Given the contents (without surrounding braces) of a JSON object, return them sorted by key. */
  private static String sortJsonObjectBodyByKey(String body) {
    java.util.List<String[]> entries = splitTopLevelEntries(body);
    if (entries.isEmpty()) {
      return body;
    }
    entries.sort(java.util.Comparator.comparing(e -> e[0]));
    StringBuilder sb = new StringBuilder(body.length());
    for (int j = 0; j < entries.size(); j++) {
      if (j > 0) sb.append(',');
      sb.append('"').append(entries.get(j)[0]).append("\":").append(entries.get(j)[1]);
    }
    return sb.toString();
  }

  /**
   * Split a JSON-object body (no enclosing braces) into [key, valueText] pairs at top level,
   * respecting nested braces, brackets, and quoted strings (with backslash escapes).
   */
  private static java.util.List<String[]> splitTopLevelEntries(String body) {
    java.util.List<String[]> entries = new java.util.ArrayList<>();
    int n = body.length();
    int i = 0;
    while (i < n) {
      while (i < n && Character.isWhitespace(body.charAt(i))) i++;
      if (i >= n) break;
      if (body.charAt(i) != '"') return java.util.Collections.emptyList();
      int keyStart = i + 1;
      int keyEnd = findStringEnd(body, i);
      if (keyEnd < 0) return java.util.Collections.emptyList();
      String key = body.substring(keyStart, keyEnd);
      i = keyEnd + 1;
      while (i < n && Character.isWhitespace(body.charAt(i))) i++;
      if (i >= n || body.charAt(i) != ':') return java.util.Collections.emptyList();
      i++;
      while (i < n && Character.isWhitespace(body.charAt(i))) i++;
      int valStart = i;
      int valEnd = findValueEnd(body, valStart);
      if (valEnd < 0) return java.util.Collections.emptyList();
      entries.add(new String[] {key, body.substring(valStart, valEnd)});
      i = valEnd;
      while (i < n && Character.isWhitespace(body.charAt(i))) i++;
      if (i < n && body.charAt(i) == ',') i++;
    }
    return entries;
  }

  private static int findStringEnd(String s, int openQuoteIdx) {
    int n = s.length();
    int i = openQuoteIdx + 1;
    while (i < n) {
      char c = s.charAt(i);
      if (c == '\\') {
        i += 2;
        continue;
      }
      if (c == '"') return i;
      i++;
    }
    return -1;
  }

  /** Returns the index AFTER the JSON value starting at {@code start}. -1 on error. */
  private static int findValueEnd(String s, int start) {
    int n = s.length();
    if (start >= n) return -1;
    char c = s.charAt(start);
    if (c == '"') {
      int e = findStringEnd(s, start);
      return e < 0 ? -1 : e + 1;
    }
    if (c == '{' || c == '[') {
      int e = matchingBraceEnd(s, start);
      return e < 0 ? -1 : e + 1;
    }
    int i = start;
    while (i < n) {
      char ch = s.charAt(i);
      if (ch == ',' || ch == '}' || ch == ']') return i;
      i++;
    }
    return n;
  }

  /**
   * Given that {@code s.charAt(open)} is '{' or '[', return the index of its matching brace.
   * Quoted strings (with backslash escapes) are skipped. Returns -1 if unbalanced.
   */
  private static int matchingBraceEnd(String s, int open) {
    char openCh = s.charAt(open);
    char closeCh = openCh == '{' ? '}' : ']';
    int depth = 0;
    int n = s.length();
    int i = open;
    while (i < n) {
      char c = s.charAt(i);
      if (c == '"') {
        int e = findStringEnd(s, i);
        if (e < 0) return -1;
        i = e + 1;
        continue;
      }
      if (c == openCh) depth++;
      else if (c == closeCh) {
        depth--;
        if (depth == 0) return i;
      }
      i++;
    }
    return -1;
  }

  private static String jsonToYaml(String json) {
    try {
      Object jsonObject = JSON_MAPPER.readValue(json, Object.class);
      return YamlFormatter.formatToYaml(jsonObject);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert JSON to YAML", e);
    }
  }

  private static String formatMessage(String expected, String actual) {
    return String.format("### Expected ###\n%s\n### Actual###\n%s\n", expected, actual);
  }
}
