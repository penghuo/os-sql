/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that LogicalProject is correctly converted to ProjectNode with column list preserved. */
class ConvertProjectTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalProject -> ProjectNode with column list preserved")
  void convertSimpleProject() {
    // source=test | fields name
    List<RexNode> projects = List.of(fieldRef(0));
    LogicalProject project =
        createProject(createTableScan("test", "name", "age"), projects, "name");

    PlanNode result = converter.convert(project);

    assertInstanceOf(ProjectNode.class, result);
    ProjectNode projectNode = (ProjectNode) result;
    assertEquals(1, projectNode.getProjections().size());
    assertInstanceOf(LuceneTableScanNode.class, projectNode.getSource());
    assertNotNull(projectNode.getOutputType());
  }

  @Test
  @DisplayName("LogicalProject with multiple columns preserves all projections")
  void convertMultiColumnProject() {
    // source=test | fields name, age
    List<RexNode> projects = List.of(fieldRef(0), fieldRef(1));
    LogicalProject project =
        createProject(createTableScan("test", "name", "age"), projects, "name", "age");

    PlanNode result = converter.convert(project);

    assertInstanceOf(ProjectNode.class, result);
    ProjectNode projectNode = (ProjectNode) result;
    assertEquals(2, projectNode.getProjections().size());
  }

  @Test
  @DisplayName("Project on top of filter creates correct structure")
  void convertProjectOverFilter() {
    RexNode predicate = greaterThan(fieldRef(1), literal(30));
    var scan = createTableScan("test", "name", "age");
    var filter = org.apache.calcite.rel.logical.LogicalFilter.create(scan, predicate);
    List<RexNode> projects = List.of(fieldRef(0));
    var project = createProject(filter, projects, "name");

    PlanNode result = converter.convert(project);

    assertInstanceOf(ProjectNode.class, result);
    assertInstanceOf(FilterNode.class, ((ProjectNode) result).getSource());
  }
}
