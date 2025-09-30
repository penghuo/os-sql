/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Abstract class to map the {@link org.opensearch.sql.storage.Table} and {@link
 * org.apache.calcite.schema.Table}
 */
public abstract class AbstractOpenSearchTable extends AbstractTable
    implements TranslatableTable, org.opensearch.sql.storage.Table {

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return new DynamicRecordTypeImpl(relDataTypeFactory);
  }
}
