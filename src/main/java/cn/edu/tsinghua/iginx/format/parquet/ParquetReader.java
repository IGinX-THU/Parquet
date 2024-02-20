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
package cn.edu.tsinghua.iginx.format.parquet;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class ParquetReader<T> implements Closeable {

  private final ParquetRecordReader<T> recordReader;

  public ParquetReader(InputFile file, Function<MessageType, RecordMaterializer<T>> recordMaterializerFactory, ParquetReadOptions options) throws IOException {
    Objects.requireNonNull(file);
    Objects.requireNonNull(recordMaterializerFactory);
    Objects.requireNonNull(options);

    ParquetMetadata footer;
    try (SeekableInputStream in = file.newStream()) {
      footer = ParquetFileReader.readFooter(file, options, in);
    }

    ParquetFileReader reader = new ParquetFileReader(file, footer, options);
    ParquetMetadata metadata = reader.getFooter();
    MessageType schema = metadata.getFileMetaData().getSchema();

    try {
      MessageType requestedSchema = options.getSchemaConvertor().apply(schema);
      reader.setRequestedSchema(requestedSchema);
    } catch (Exception e) {
      reader.close();
      throw e;
    }

    RecordMaterializer<T> recordMaterializer = recordMaterializerFactory.apply(schema);
    Objects.requireNonNull(recordMaterializer);
    this.recordReader = new ParquetRecordReader<>(recordMaterializer, reader, options);
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  /**
   * @return the next record or null if finished
   * @throws IOException if there is an error while reading
   */
  public T read() throws IOException {
    if (!recordReader.nextKeyValue()) {
      return null;
    }
    return recordReader.getCurrentValue();
  }

  /**
   * @return the row index of the last read row. If no row has been processed, returns -1.
   */
  public long getCurrentRowIndex() {
    return recordReader.getCurrentRowIndex();
  }

  public ParquetMetadata getMetadata() {
    return recordReader.getReader().getFooter();
  }
}
