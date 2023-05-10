/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.mapping

import org.apache.spark.sql.types.{DataType, DataTypes, SQLUserDefinedType, UserDefinedType}

trait OpenSearchDataTypes {

  def sqlDataType(): DataType
}

@SQLUserDefinedType(udt = classOf[OpenSearchTextUDT])
case class OpenSearchText(text: String) extends Serializable {

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match {
    case v: OpenSearchText => this.text == v.text
    case _ => false
  }

  override def toString: String = text
}

// does UDT really required?
case class OpenSearchTextUDT() extends UserDefinedType[OpenSearchText] {

  override def sqlType: DataType = DataTypes.StringType

  override def serialize(obj: OpenSearchText): Any = obj.text

  override def deserialize(datum: Any): OpenSearchText = datum match {
    case s: String => OpenSearchText(s)
    case _ => throw new RuntimeException(s"Can't parse $datum into OpenSearchText");
  }

  override def userClass: Class[OpenSearchText] = classOf[OpenSearchText]
}

object OpenSearchTextUDT extends OpenSearchTextUDT
