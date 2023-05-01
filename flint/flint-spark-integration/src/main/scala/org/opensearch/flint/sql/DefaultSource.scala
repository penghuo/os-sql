package org.opensearch.flint.sql

import org.apache.spark.sql.{OpenSearchOptions, OpenSearchRelation, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    OpenSearchRelation(new OpenSearchOptions(parameters))(sqlContext.sparkSession)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    OpenSearchRelation(new OpenSearchOptions(parameters), Some(schema))(sqlContext.sparkSession)
  }

  override def shortName(): String = "opensearch"
}
