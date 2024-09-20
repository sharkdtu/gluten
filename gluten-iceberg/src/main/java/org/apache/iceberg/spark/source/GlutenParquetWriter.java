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

import org.apache.gluten.datasource.NativeBatchWriteInfo;
import org.apache.gluten.execution.datasource.GlutenParquetWriterInjects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.GlutenOutputWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;

public class GlutenParquetWriter implements FileAppender<InternalRow>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenParquetWriter.class);
  private GlutenOutputWriter nativeWriter;
  private Schema schema;
  private final String filePath;
  private long length;
  private boolean closed = false;
  private final Configuration hadoopConfiguration;

  public GlutenParquetWriter(
      String filePath, StructType dataSchema, Map<String, String> nativeConf, Schema schema) {
    SparkConf sparkConf = SparkEnv.get().conf();
    this.hadoopConfiguration = SparkHadoopUtil.get().newConfiguration(sparkConf);

    this.nativeWriter =
        (GlutenOutputWriter)
            GlutenParquetWriterInjects.getInstance()
                .createOutputWriter(filePath, dataSchema, hadoopConfiguration, nativeConf);

    this.filePath = filePath;
    this.length = 0;
    this.schema = schema;
  }

  /** @param row row here is FakeRow. */
  @Override
  public void add(InternalRow row) {
    NativeBatchWriteInfo info = nativeWriter.writeAndCollectInfo(row);
    this.length = info.numBytes;
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Cannot return metrics for unclosed writer");

    long startMs = System.currentTimeMillis();
    Path path = new Path(filePath);

    Metrics metrics =
        ParquetUtil.fileMetrics(
            HadoopInputFile.fromPath(path, hadoopConfiguration), MetricsConfig.getDefault());
    LOG.info("Collecting metrics costs {} ms.", System.currentTimeMillis() - startMs);
    return metrics;
  }

  @Override
  public long length() {
    return this.length;
  }

  @Override
  public void close() {
    if (!closed) {
      NativeBatchWriteInfo info = nativeWriter.closeAndCollectInfo();
      this.length = info.numBytes;
      closed = true;
    }
  }
}
