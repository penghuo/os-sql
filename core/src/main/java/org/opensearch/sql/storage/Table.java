/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;

/** Table. */
public interface Table {

  /**
   * Check if current table exists.
   *
   * @return true if exists, otherwise false
   */
  default boolean exists() {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  /**
   * Create table given table schema.
   *
   * @param schema table schema
   */
  default void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException("Unsupported Operation");
  }

  /** Get the {@link ExprType} for each field in the table. */
  Map<String, ExprType> getFieldTypes();

  /** Get the {@link ExprType} for each meta-field (reserved fields) in the table. */
  default Map<String, ExprType> getReservedFieldTypes() {
    return Map.of();
  }

  /**
   * The container hierarchy declared by the table's schema: one entry for every field name in
   * {@link #getFieldTypes()}, mapping it to the names of the container (object/nested) fields that
   * declare it, outermost first. A field declared at the root of the schema maps to an empty list.
   *
   * <p>This is a structural relationship read from the schema, not a projection list, and it is the
   * only authority on which field is nested inside which. It never infers structure from the field
   * name, so it stays correct when a schema declares a single field whose name itself contains dots
   * — as OpenSearch does for an object mapped with {@code disable_objects: true}, where {@code
   * attributes} declares one child literally named {@code log.file.path}, and {@code
   * attributes.log.file.path} therefore has the single ancestor {@code attributes}.
   *
   * <p>The default is empty, meaning the table declares no hierarchy and callers must fall back to
   * their own convention.
   */
  default Map<String, List<String>> getFieldAncestors() {
    return Map.of();
  }

  /**
   * Implement a {@link LogicalPlan} by {@link PhysicalPlan} in storage engine.
   *
   * @param plan logical plan
   * @return physical plan
   * @deprecated because of new {@link TableScanBuilder} implementation
   */
  @Deprecated(since = "2.5.0")
  PhysicalPlan implement(LogicalPlan plan);

  /**
   * Optimize the {@link LogicalPlan} by storage engine rule. The default optimize solution is no
   * optimization.
   *
   * @param plan logical plan.
   * @return logical plan.
   * @deprecated because of new {@link TableScanBuilder} implementation
   */
  @Deprecated(since = "2.5.0")
  default LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

  /**
   * Create table scan builder for logical to physical transformation.
   *
   * @return table scan builder
   */
  default TableScanBuilder createScanBuilder() {
    return null; // TODO: Enforce all subclasses to implement this later
  }

  /*
   * Create table write builder for logical to physical transformation.
   *
   * @param plan logical write plan
   * @return table write builder
   */
  default TableWriteBuilder createWriteBuilder(LogicalWrite plan) {
    throw new UnsupportedOperationException("Write operation is not supported on current table");
  }

  /** Translate {@link Table} to {@link StreamingSource} if possible. */
  default StreamingSource asStreamingSource() {
    throw new UnsupportedOperationException();
  }
}
