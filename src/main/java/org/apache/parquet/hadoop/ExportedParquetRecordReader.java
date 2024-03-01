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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class ExportedParquetRecordReader<T> implements Closeable {
  private final InternalParquetRecordReader<T> internalReader;
  private final ParquetFileReader reader;
  private boolean isEnd = false;

  public ExportedParquetRecordReader(
      RecordMaterializer<T> recordMaterializer,
      ParquetFileReader reader,
      MessageType requestedSchema,
      ParquetReadOptions options) {
    FilterCompat.Filter filter =
        options.getRecordFilter() == null || !options.useRecordFilter()
            ? FilterCompat.NOOP
            : options.getRecordFilter();

    ReadSupport<T> readSupport = new DelegateReadSupport<>(recordMaterializer, requestedSchema);

    this.reader = reader;
    this.internalReader = new InternalParquetRecordReader<>(
        readSupport,
        filter);

    internalReader.initialize(reader, options);
  }

  @Override
  public void close() throws IOException {
    internalReader.close();
  }

  public ParquetFileReader getReader() {
    return reader;
  }

  public T getCurrentValue() throws IOException {
    try {
      return internalReader.getCurrentValue();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public float getProgress() throws IOException {
    try {
      return internalReader.getProgress();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean nextKeyValue() throws IOException {
    try {
      boolean hasNext = internalReader.nextKeyValue();
      if (!hasNext) {
        isEnd = true;
      }
      return hasNext;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the row index of the current row. If no row has been processed or if the
   * row index information is unavailable, returns -1.
   */
  public long getCurrentRowIndex() {
    if (isEnd) {
      return -1;
    }
    return internalReader.getCurrentRowIndex();
  }

  private static class DelegateReadSupport<T> extends ReadSupport<T> {
    private final RecordMaterializer<T> recordMaterializer;
    private final MessageType requestedSchema;

    public DelegateReadSupport(RecordMaterializer<T> recordMaterializer, MessageType requestedSchema) {
      this.recordMaterializer = recordMaterializer;
      this.requestedSchema = requestedSchema;
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(requestedSchema);
    }

    @Override
    public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
      return recordMaterializer;
    }
  }
}
