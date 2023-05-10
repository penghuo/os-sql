/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.mapping

import org.json4s.{JField, JString, _}
import org.json4s.jackson._
import org.opensearch.flint.storage.{FlintClient, FlintOptions}

import org.apache.spark.sql.flint.mapping.FlintMapping.compile
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, MetadataBuilder, StringType, StructType}

class FlintMapping(client: FlintClient, options: FlintOptions) {

  def inferSchema(index: String): StructType = {
    val mappings = client.getMappings(index)
    compile(JsonMethods.parse(mappings))
  }
}

object FlintMapping {
  def compile(properties: JValue): StructType = {
    val fields = for {
      JObject(field) <- properties \ "properties"
      JField(fieldName, fieldMapping @ JObject(fieldProperties)) <- field
      metadataBuilder = new MetadataBuilder()
      dataType = fieldProperties
        .collectFirst {
          case JField("type", JString(fieldType)) =>
            fieldType match {
              // common types
              case "integer" => IntegerType
              case "long" => LongType
              case "keyword" => StringType

              // text search type
              case "text" =>
                metadataBuilder.putString("osType", "text")
                StringType

              // object types
              case "object" => compile(fieldMapping \ "properties")

              case _ => throw new IllegalStateException(s"unsupported field type $fieldType")
            }
          case JField("properties", subProperties @ JObject(_)) => compile(subProperties)
        }
        .getOrElse(StringType)
    } yield DataTypes.createStructField(fieldName, dataType, true, metadataBuilder.build())

    StructType(fields)
  }
}
