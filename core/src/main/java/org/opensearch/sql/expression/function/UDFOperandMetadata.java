/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * This class is created for the compatibility with {@link SqlUserDefinedFunction} constructors when
 * creating UDFs, so that a type checker can be passed to the constructor of {@link
 * SqlUserDefinedFunction} as a {@link SqlOperandMetadata}.
 */
public interface UDFOperandMetadata extends SqlOperandMetadata {
  SqlOperandTypeChecker getInnerTypeChecker();

  static UDFOperandMetadata wrap(FamilyOperandTypeChecker typeChecker) {
    return new UDFOperandMetadata() {
      @Override
      public SqlOperandTypeChecker getInnerTypeChecker() {
        return typeChecker;
      }

      @Override
      public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public List<String> paramNames() {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return typeChecker.checkOperandTypesWithoutTypeCoercion(callBinding, throwOnFailure);
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return typeChecker.getOperandCountRange();
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return typeChecker.getAllowedSignatures(op, opName);
      }
    };
  }

  static UDFOperandMetadata wrap(CompositeOperandTypeChecker typeChecker) {
    for (SqlOperandTypeChecker rule : typeChecker.getRules()) {
      if (!(rule instanceof FamilyOperandTypeChecker)) {
        throw new IllegalArgumentException(
            "Currently only compositions of ImplicitCastOperandTypeChecker are supported");
      }
    }

    return new UDFOperandMetadata() {
      @Override
      public SqlOperandTypeChecker getInnerTypeChecker() {
        return typeChecker;
      }

      @Override
      public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public List<String> paramNames() {
        // This function is not used in the current context, so we return an empty list.
        return Collections.emptyList();
      }

      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return typeChecker.checkOperandTypes(callBinding, throwOnFailure);
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return typeChecker.getOperandCountRange();
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return typeChecker.getAllowedSignatures(op, opName);
      }
    };
  }

  static UDFOperandMetadata wrapUDT(List<List<RelDataType>> allowSignatures) {
    return new UDTOperandMetadata(allowSignatures);
  }

  /**
   * Permissive variadic operand metadata: accepts any number of operands of any type. Used by UDFs
   * whose visitor-side validation is the source of truth (e.g. lambda collection UDFs like {@code
   * array}, {@code mvappend}). Without explicit metadata, the SqlValidator round-trip reaches
   * {@code SqlOperator.getOperandCountRange}'s default which throws {@code
   * UnsupportedOperationException: class UserDefinedFunctionBuilder$1: <name>}.
   */
  static UDFOperandMetadata permissiveVariadic() {
    return PERMISSIVE_VARIADIC;
  }

  UDFOperandMetadata PERMISSIVE_VARIADIC =
      new UDFOperandMetadata() {
        @Override
        public org.apache.calcite.sql.type.SqlOperandTypeChecker getInnerTypeChecker() {
          return new org.apache.calcite.sql.type.SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
              return true;
            }

            @Override
            public org.apache.calcite.sql.SqlOperandCountRange getOperandCountRange() {
              return org.apache.calcite.sql.type.SqlOperandCountRanges.from(0);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
              return opName + "(...)";
            }
          };
        }

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
          return Collections.emptyList();
        }

        @Override
        public List<String> paramNames() {
          return Collections.emptyList();
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
          return true;
        }

        @Override
        public org.apache.calcite.sql.SqlOperandCountRange getOperandCountRange() {
          return org.apache.calcite.sql.type.SqlOperandCountRanges.from(0);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(...)";
        }
      };

  record UDTOperandMetadata(List<List<RelDataType>> allowedParamTypes)
      implements UDFOperandMetadata {
    @Override
    public SqlOperandTypeChecker getInnerTypeChecker() {
      return this;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
      return List.of();
    }

    @Override
    public List<String> paramNames() {
      return List.of();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      int actual = callBinding.getOperandCount();
      for (List<RelDataType> sig : allowedParamTypes) {
        if (sig.size() == actual) {
          return true;
        }
      }
      return false;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
      for (List<RelDataType> sig : allowedParamTypes) {
        min = Math.min(min, sig.size());
        max = Math.max(max, sig.size());
      }
      if (allowedParamTypes.isEmpty()) {
        min = 0;
        max = 0;
      }
      final int finalMin = min, finalMax = max;
      return new SqlOperandCountRange() {
        @Override
        public boolean isValidCount(int count) {
          return count >= finalMin && count <= finalMax;
        }

        @Override
        public int getMin() {
          return finalMin;
        }

        @Override
        public int getMax() {
          return finalMax;
        }
      };
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return "";
    }
  }
}
