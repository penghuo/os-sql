/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.Predicate

/**
 * Todo. find the right package.
 */
object FlintQueryBuilder {

  /**
   * Using AND to concat predicates.
   */
  def compilePredicates(predicates: Array[Predicate]): Option[String] = {
    if (predicates.isEmpty) {
      return None
    }

    val filters = predicates.map(compilePredicate).filter(_.isDefined).map(_.get)

    Some(s"""{
            |  "bool": {
            |    "filter": [
            |      ${filters.mkString(",")}
            |    ]
            |  }
            |}""".stripMargin)
  }

  def compilePredicate(expr: Predicate): Option[String] = {
    try {
      val name = expr.name()
      name match {
        case "=" =>
          val fieldName = compile(expr.children()(0))
          val value = compile(expr.children()(1))
          Some(s"""{
                  |    "term": {
                  |      "$fieldName": {
                  |        "value": $value
                  |      }
                  |    }
                  |}""")
        case _ => None
      }
    } catch {
      case _: Throwable => None
    }
  }

  def compile(expr: Expression): String = {
    expr match {
      case LiteralValue(value, _) => value.toString
      case f: FieldReference => f.toString()
      case _ => throw new IllegalStateException(s"unexpected expression type ${expr.getClass}")
    }
  }
}
