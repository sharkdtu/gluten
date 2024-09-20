/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark.source

import org.apache.gluten.execution.DataSourceWriteAnalyzeRegister

import org.apache.spark.sql.connector.write.Write

class IcebergWriteAnalyzeProvider extends DataSourceWriteAnalyzeRegister {
  import IcebergWriteAnalyzeProvider._

  override def getWriteInfo(write: Write): WriteInfo = {
    write match {
      case _: SparkWrite =>
        WriteInfo(Some(WRITE_FACTORY_CLASS), Some(INTERNAL_ROW_WRAPPER_CLASS))
      case _ => throw new IllegalArgumentException(s"unknown write type ${write.getClass.getName}")
    }
  }

  override val writeClassName: String = "org.apache.iceberg.spark.source.SparkWriteBuilder"
}

object IcebergWriteAnalyzeProvider {
  private val WRITE_FACTORY_CLASS = "org.apache.iceberg.spark.source.GlutenSparkFileWriterFactory"
  private val INTERNAL_ROW_WRAPPER_CLASS =
    "org.apache.iceberg.spark.source.GlutenInternalRowWrapper"
}
