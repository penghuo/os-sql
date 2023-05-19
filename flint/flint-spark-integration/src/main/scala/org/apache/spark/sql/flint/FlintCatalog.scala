/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FlintCatalog extends FunctionCatalog {

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    Array(Identifier.of(namespace, "array_contains"))
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    ArrayContains()
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // do nothing
  }

  override def name(): String = "flint"
}

case class ArrayContains() extends UnboundFunction {
  override def name(): String = "array_contains"

  override def bind(inputType: StructType): BoundFunction = {
    inputType match {
      case StructType(Array(l, r)) =>
        (l.dataType, r.dataType) match {
          case (ArrayType(e1, _), e2) if e1.sameType(e2) => ArrayContainsFunction(l, r)
          case _ => throw new UnsupportedOperationException("type mismatched " + inputType)
        }
      case _ =>
        throw new UnsupportedOperationException("type mismatched " + inputType)
    }
  }

  override def description(): String =
    "array_contains: return true if array_contains the data. array_contains(ARRAY(T), T) -> boolean"
}

case class ArrayContainsFunction(left: StructField, right: StructField)
    extends ScalarFunction[Boolean] {

  override def inputTypes(): Array[DataType] = {
    (left.dataType, right.dataType) match {
      case (_, NullType) => Array.empty
      case (ArrayType(e1, hasNull), e2) if e1.sameType(e2) => Array(ArrayType(e1, hasNull), e2)
      case _ => Array.empty
    }
  }
  override def resultType(): DataType = IntegerType
  override def name(): String = "ARRAY_CONTAINS"
  override def canonicalName(): String = "flint.array_contains"
}
