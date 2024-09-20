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
package org.apache.iceberg.spark.source;

import com.google.common.collect.Maps;
import org.apache.iceberg.*;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GlutenSparkFileWriterFactory extends SparkFileWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenSparkFileWriterFactory.class);

  private FileFormat dataFileFormat;

  private Map<String, String> nativeConf;
  private final SortOrder dataSortOrder;

  public GlutenSparkFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      StructType dataSparkType,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      StructType equalityDeleteSparkType,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema,
      StructType positionDeleteSparkType,
      String fileStatsRowCount,
      Map<String, String> writeProperties) {
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSparkType,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSparkType,
        equalityDeleteSortOrder,
        positionDeleteRowSchema,
        positionDeleteSparkType,
        fileStatsRowCount,
        writeProperties);

    this.dataFileFormat = dataFileFormat;
    this.dataSortOrder = dataSortOrder;

    this.nativeConf = Maps.newHashMap();
    if (writeProperties.containsKey(TableProperties.PARQUET_COMPRESSION)) {
      this.nativeConf.put(
          SQLConf.PARQUET_COMPRESSION().key(),
          writeProperties.get(TableProperties.PARQUET_COMPRESSION));
    }
  }

  @Override
  public DataWriter<InternalRow> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    OutputFile outputFile = file.encryptingOutputFile();
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();

    if (this.dataFileFormat == FileFormat.PARQUET) {
      return GlutenParquet.DataWriterBuilder.writeData(outputFile)
          .withDataSchema(SparkSchemaUtil.convert(dataSchema()))
          .withSpec(spec)
          .withPartition(partition)
          .withKeyMetadata(keyMetadata)
          .withSchema(table().schema())
          .withNativeConf(nativeConf)
          .withSortOrder(dataSortOrder)
          .build();
    } else {
      throw new UnsupportedOperationException("Only support parquet currently");
    }
  }
}
