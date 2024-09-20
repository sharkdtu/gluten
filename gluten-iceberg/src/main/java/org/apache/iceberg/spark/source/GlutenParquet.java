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

import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class GlutenParquet {

  public static class DataWriterBuilder {
    private final String location;
    private PartitionSpec spec = null;
    private StructLike partition = null;
    private EncryptionKeyMetadata keyMetadata = null;
    private StructType structType;
    private Map<String, String> nativeConf = new HashMap<>();
    private Schema schema;
    private SortOrder sortOrder;

    DataWriterBuilder(OutputFile file) {
      location = file.location();
    }

    DataWriterBuilder withSpec(PartitionSpec spec) {
      this.spec = spec;
      return this;
    }

    DataWriterBuilder withPartition(StructLike partition) {
      this.partition = partition;
      return this;
    }

    public DataWriterBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public static DataWriterBuilder writeData(OutputFile file) {
      return new DataWriterBuilder(file);
    }

    public DataWriterBuilder withDataSchema(StructType structType) {
      this.structType = structType;
      return this;
    }

    public DataWriterBuilder withSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public DataWriterBuilder withNativeConf(Map<String, String> nativeConf) {
      this.nativeConf = nativeConf;
      return this;
    }

    public DataWriterBuilder withSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    public DataWriter<InternalRow> build() {
      GlutenParquetWriter appender =
          new GlutenParquetWriter(location, structType, nativeConf, schema);
      FileFormat fileFormat = FileFormat.PARQUET;
      return new DataWriter<>(
          appender,
          fileFormat,
          location,
          spec,
          partition,
          keyMetadata,
          sortOrder,
          schema.schemaId());
    }
  }
}
