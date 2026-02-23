/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.boundary;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BI-5: Dependency boundary enforcement.
 *
 * <p>Validates that no DQE module (org.opensearch.dqe.*) transitively depends on disallowed
 * packages from the existing SQL/PPL/Calcite engine. This check runs in CI and blocks merges on
 * violation.
 *
 * <p>Disallowed packages:
 *
 * <ul>
 *   <li>org.opensearch.sql.sql.* — existing SQL grammar, parser, or AST
 *   <li>org.opensearch.sql.ppl.* — PPL grammar, parser, or AST
 *   <li>org.opensearch.sql.calcite.* — Calcite-based planner, optimizer, or execution
 *   <li>org.opensearch.sql.expression.* — existing expression evaluation framework
 *   <li>org.opensearch.sql.planner.* — existing query planner
 *   <li>org.opensearch.sql.executor.* — existing query executor
 *   <li>org.opensearch.sql.storage.* — existing storage abstraction
 * </ul>
 */
class DqeDependencyBoundaryTest {

  private static JavaClasses dqeClasses;

  @BeforeAll
  static void importClasses() {
    dqeClasses =
        new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("org.opensearch.dqe");
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.sql (existing SQL parser/AST)")
  void noDependencyOnSqlModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.sql..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.ppl (PPL parser/AST)")
  void noDependencyOnPplModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.ppl..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.calcite (Calcite engine)")
  void noDependencyOnCalciteModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.calcite..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName(
      "DQE modules must not depend on org.opensearch.sql.expression (expression framework)")
  void noDependencyOnExpressionModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.expression..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.planner (query planner)")
  void noDependencyOnPlannerModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.planner..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.executor (query executor)")
  void noDependencyOnExecutorModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.executor..");
    rule.check(dqeClasses);
  }

  @Test
  @DisplayName("DQE modules must not depend on org.opensearch.sql.storage (storage abstraction)")
  void noDependencyOnStorageModule() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("org.opensearch.dqe..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.opensearch.sql.storage..");
    rule.check(dqeClasses);
  }
}
