/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.DISTINCT_COUNT_APPROX;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

public class OpenSearchBuiltinOperators extends ReflectiveSqlOperatorTable {

  private static final Supplier<OpenSearchBuiltinOperators> INSTANCE =
      Suppliers.memoize(() -> (OpenSearchBuiltinOperators) new OpenSearchBuiltinOperators().init());

  public static final SqlAggFunction approxDistinctCountFunction =
      UserDefinedFunctionUtils.createUserDefinedAggFunction(
          DistinctCountApproxAggFunction.class,
          DISTINCT_COUNT_APPROX.toString(),
          ReturnTypes.BIGINT_FORCE_NULLABLE,
          PPLOperandTypes.OPTIONAL_ANY);

  public static OpenSearchBuiltinOperators instance() {
    return INSTANCE.get();
  }
}
