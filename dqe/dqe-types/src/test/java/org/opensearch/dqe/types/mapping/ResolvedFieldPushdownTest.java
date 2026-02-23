/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("ResolvedField pushdown field selection (T-3)")
class ResolvedFieldPushdownTest {

  @Nested
  @DisplayName("getPushdownFieldPath")
  class PushdownFieldPath {

    @Test
    @DisplayName("Text with keyword sub-field: EQUALITY uses .keyword")
    void equalityUsesKeyword() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title.keyword", field.getPushdownFieldPath(ResolvedField.FieldUsage.EQUALITY));
    }

    @Test
    @DisplayName("Text with keyword sub-field: IN uses .keyword")
    void inUsesKeyword() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title.keyword", field.getPushdownFieldPath(ResolvedField.FieldUsage.IN));
    }

    @Test
    @DisplayName("Text with keyword sub-field: SORT uses .keyword")
    void sortUsesKeyword() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title.keyword", field.getPushdownFieldPath(ResolvedField.FieldUsage.SORT));
    }

    @Test
    @DisplayName("Text with keyword sub-field: RANGE uses .keyword")
    void rangeUsesKeyword() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title.keyword", field.getPushdownFieldPath(ResolvedField.FieldUsage.RANGE));
    }

    @Test
    @DisplayName("Text with keyword sub-field: FULL_TEXT uses text field")
    void fullTextUsesTextField() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title", field.getPushdownFieldPath(ResolvedField.FieldUsage.FULL_TEXT));
    }

    @Test
    @DisplayName("Text with keyword sub-field: PROJECTION uses text field")
    void projectionUsesTextField() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title", field.getPushdownFieldPath(ResolvedField.FieldUsage.PROJECTION));
    }

    @Test
    @DisplayName("Field without keyword sub-field always uses base path")
    void noKeywordAlwaysUsesBasePath() {
      ResolvedField field = new ResolvedField("age", DqeTypes.INTEGER, true, null, false, false);
      assertEquals("age", field.getPushdownFieldPath(ResolvedField.FieldUsage.EQUALITY));
      assertEquals("age", field.getPushdownFieldPath(ResolvedField.FieldUsage.SORT));
      assertEquals("age", field.getPushdownFieldPath(ResolvedField.FieldUsage.FULL_TEXT));
    }

    @Test
    @DisplayName("Custom keyword sub-field name (e.g., .raw)")
    void customKeywordSubFieldName() {
      ResolvedField field =
          new ResolvedField(
              "description", DqeTypes.VARCHAR, false, "description.raw", false, false);
      assertEquals(
          "description.raw", field.getPushdownFieldPath(ResolvedField.FieldUsage.EQUALITY));
      assertEquals("description", field.getPushdownFieldPath(ResolvedField.FieldUsage.FULL_TEXT));
    }
  }

  @Nested
  @DisplayName("getSortFieldPath")
  class SortFieldPath {

    @Test
    @DisplayName("Text with keyword sub-field sorts on .keyword")
    void textWithKeywordSortsOnKeyword() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertEquals("title.keyword", field.getSortFieldPath());
    }

    @Test
    @DisplayName("Text with fielddata sorts on field itself")
    void textWithFielddataSortsOnField() {
      ResolvedField field =
          new ResolvedField("tags", DqeTypes.VARCHAR, true, "tags.keyword", true, false);
      // Has keyword sub-field, so sorts on that
      assertEquals("tags.keyword", field.getSortFieldPath());
    }

    @Test
    @DisplayName("Sortable field without keyword sorts on field path")
    void sortableFieldSortsOnFieldPath() {
      ResolvedField field = new ResolvedField("age", DqeTypes.INTEGER, true, null, false, false);
      assertEquals("age", field.getSortFieldPath());
    }

    @Test
    @DisplayName("Unsortable field without keyword returns null")
    void unsortableReturnsNull() {
      ResolvedField field = new ResolvedField("geo", DqeTypes.VARCHAR, false, null, false, false);
      assertNull(field.getSortFieldPath());
    }
  }

  @Nested
  @DisplayName("hasKeywordSubField")
  class HasKeywordSubField {

    @Test
    @DisplayName("Returns true when keyword sub-field present")
    void trueWhenPresent() {
      ResolvedField field =
          new ResolvedField("title", DqeTypes.VARCHAR, false, "title.keyword", false, false);
      assertTrue(field.hasKeywordSubField());
    }

    @Test
    @DisplayName("Returns false when no keyword sub-field")
    void falseWhenAbsent() {
      ResolvedField field = new ResolvedField("age", DqeTypes.INTEGER, true, null, false, false);
      assertFalse(field.hasKeywordSubField());
    }
  }

  @Nested
  @DisplayName("FieldUsage.prefersKeyword")
  class FieldUsagePrefersKeyword {

    @Test
    @DisplayName("EQUALITY, IN, SORT, RANGE prefer keyword")
    void keywordPreferred() {
      assertTrue(ResolvedField.FieldUsage.EQUALITY.prefersKeyword());
      assertTrue(ResolvedField.FieldUsage.IN.prefersKeyword());
      assertTrue(ResolvedField.FieldUsage.SORT.prefersKeyword());
      assertTrue(ResolvedField.FieldUsage.RANGE.prefersKeyword());
    }

    @Test
    @DisplayName("FULL_TEXT and PROJECTION do not prefer keyword")
    void keywordNotPreferred() {
      assertFalse(ResolvedField.FieldUsage.FULL_TEXT.prefersKeyword());
      assertFalse(ResolvedField.FieldUsage.PROJECTION.prefersKeyword());
    }
  }
}
