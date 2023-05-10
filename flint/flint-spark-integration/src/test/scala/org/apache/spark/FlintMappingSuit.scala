/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.json4s.jackson.JsonMethods

import org.apache.spark.sql.flint.mapping.FlintMapping
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class FlintMappingSuit extends FlintSuite {
  test("parse index mapping") {
    val mapping = """{
                    |  "properties": {
                    |    "id": {
                    |      "type": "keyword"
                    |    },
                    |    "value": {
                    |      "type": "integer"
                    |    }
                    |  }
                    |}""".stripMargin
    val actualSchema = FlintMapping.compile(JsonMethods.parse(mapping))
    val expectedSchema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("value", IntegerType, nullable = true)))
    assertResult(expectedSchema)(actualSchema)
  }
}
