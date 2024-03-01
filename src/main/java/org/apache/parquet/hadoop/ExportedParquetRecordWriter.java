/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import cn.edu.tsinghua.iginx.format.parquet.api.RecordDematerializer;
import cn.edu.tsinghua.iginx.format.parquet.codec.DefaultCodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class ExportedParquetRecordWriter<T> implements Closeable {
  private final ParquetFileWriter parquetFileWriter;
  private final InternalParquetRecordWriter<T> internalWriter;

  public ExportedParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      RecordDematerializer<T> recordDematerializer,
      MessageType schema,
      Map<String, String> extraMetaData,
      long rowGroupSize,
      BytesInputCompressor compressor,
      boolean validating,
      ParquetProperties props) {
    this.parquetFileWriter = parquetFileWriter;
    this.internalWriter = new InternalParquetRecordWriter<>(
        parquetFileWriter,
        new DelegateWriteSupport<>(recordDematerializer, schema, extraMetaData),
        schema,
        extraMetaData,
        rowGroupSize,
        DefaultCodecFactory.wrap(compressor),
        validating,
        props);
  }

  @Override
  public void close() throws IOException {
    try {
      internalWriter.close();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void write(T value) throws IOException {
    try {
      internalWriter.write(value);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return internalWriter.getDataSize();
  }

  public ParquetFileWriter getWriter() {
    return parquetFileWriter;
  }

  private static class DelegateWriteSupport<T> extends WriteSupport<T> {

    private final RecordDematerializer<T> recordDematerializer;
    private final MessageType schema;
    private final Map<String, String> extraMetaData;

    private DelegateWriteSupport(RecordDematerializer<T> recordDematerializer, MessageType schema, Map<String, String> extraMetaData) {
      this.recordDematerializer = recordDematerializer;
      this.schema = schema;
      this.extraMetaData = extraMetaData;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(schema, extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      recordDematerializer.prepare(recordConsumer);
    }

    @Override
    public void write(T record) {
      recordDematerializer.write(record);
    }
  }

}
